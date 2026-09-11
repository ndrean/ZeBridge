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
const pgoutput = @import("pgoutput.zig");
const nats = @import("nats");
const topology_mod = @import("topology.zig");
const c_imports = @import("c_imports.zig");
const c = c_imports.c;

const log = std.log.scoped(.generation_producer);

pub const GenerationProducer = struct {
    allocator: std.mem.Allocator,
    pg_config: *const pg_conn.PgConf,
    /// Where zebridge_generations is READ AND WRITTEN: the reader on a primary, the
    /// writer when the reader is a standby (§10cz) — the chain's own rows must be
    /// read back from where they were written, never through replication lag.
    book_config: *const pg_conn.PgConf,
    should_stop: *std.atomic.Value(bool),
    io: std.Io,
    endpoint: config.Nats.Endpoint,
    /// GENERATION_RULES — a RESTRICTION intersected with the derived set (probes,
    /// dev subsets). Empty means: everything the publication carries.
    rules: *const config.EventClassification.TransitionRules,
    /// table → [version_col, …] (SYNC_RULES); absent falls back to the default column
    sync_rules: *const config.EventClassification.TransitionRules,
    /// table → [tenant_col] (TENANT_RULES) — which tables are tenant-scoped, and by
    /// which column; the tenant SET itself comes from the data (zebridge_tenants_of).
    tenant_rules: *const config.EventClassification.TransitionRules,
    /// For isCdcRoutable (skip tables no client can follow) and the open tenant.
    topo: *const topology_mod.Topology,
    /// The publication IS the list (the BRIDGE_CDC_TABLES lesson): membership is
    /// derived from pg_publication_tables each tick, so zebridge_enable() cascades
    /// to generations with no second list existing to disagree.
    publication_name: []const u8,
    cadence_seconds: u64,
    chain_depth: u32,
    /// §10ej: set by `buildOne` when the cut it just published had already fallen
    /// off the stream by the time the manifest was live — read by the tick's retry.
    cut_fell_off: bool = false,
    /// The CDC per-event buffer (2^BASE_BUF). The chain has no per-row ceiling —
    /// object chunking removes it — so the producer is where a row too wide for
    /// CDC gets DETECTED (the retirement survivor of the snapshot path's
    /// measureWidestRow): measured for free while encoding, warned loudly.
    event_buf_bytes: usize,
    thread: ?std.Thread = null,

    pub fn init(
        allocator: std.mem.Allocator,
        pg_config: *const pg_conn.PgConf,
        book_config: *const pg_conn.PgConf,
        should_stop: *std.atomic.Value(bool),
        io: std.Io,
        endpoint: config.Nats.Endpoint,
        rules: *const config.EventClassification.TransitionRules,
        sync_rules: *const config.EventClassification.TransitionRules,
        tenant_rules: *const config.EventClassification.TransitionRules,
        topo: *const topology_mod.Topology,
        publication_name: []const u8,
        cadence_seconds: u64,
        chain_depth: u32,
        event_buf_bytes: usize,
    ) GenerationProducer {
        return .{
            .allocator = allocator,
            .pg_config = pg_config,
            .book_config = book_config,
            .should_stop = should_stop,
            .io = io,
            .endpoint = endpoint,
            .rules = rules,
            .sync_rules = sync_rules,
            .tenant_rules = tenant_rules,
            .topo = topo,
            .publication_name = publication_name,
            .cadence_seconds = cadence_seconds,
            .chain_depth = chain_depth,
            .event_buf_bytes = event_buf_bytes,
        };
    }

    pub fn start(self: *GenerationProducer) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    pub fn join(self: *GenerationProducer) void {
        if (self.thread) |t| t.join();
        self.thread = null;
    }

    /// §10du: a tick asked for out of cadence. Set by the catalogue reload when a
    /// table's seed epoch moves — every client of that table is waiting, empty, for
    /// the full under the new epoch, and the cadence is minutes. One producer per
    /// process, so a module-level flag is the whole channel; the sleep loop checks it
    /// once a second and ticks at once.
    pub var kick_requested = std.atomic.Value(bool).init(false);
    pub fn kick() void {
        kick_requested.store(true, .release);
    }

    fn run(self: *GenerationProducer) void {
        log.info("🧬 Generation producer started: deriving from publication '{s}' ({s}), cadence {d}s, chain depth {d}", .{
            self.publication_name,
            if (self.rules.count() > 0) "RESTRICTED by GENERATION_RULES" else "every published table",
            self.cadence_seconds,
            self.chain_depth,
        });
        // First tick immediately: an operator enabling generations should not wait a
        // full cadence to learn whether the configuration works.
        while (!self.should_stop.load(.acquire)) {
            self.tick() catch |err| log.err("🧬 generation tick failed: {}", .{err});
            var slept: u64 = 0;
            while (slept < self.cadence_seconds and !self.should_stop.load(.acquire)) : (slept += 1) {
                if (kick_requested.swap(false, .acq_rel)) {
                    log.info("🧬 kicked: a seed epoch moved — cutting the next generation now, not at the cadence", .{});
                    break;
                }
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
        // The WHOLE connection renders timestamps in UTC, not just the snapshot
        // transaction: the window query that rebuilds manifest entries reads stored
        // cutoffs OUTSIDE the transaction, and a session-timezone render there mixed
        // `+02` and `+00` strings inside one manifest (measured live in the
        // live-birth exercise: full.cutoff +02, delta cutoff +00). Chain bounds are
        // STRING-compared — one canonical form or nothing, same law as payloads.
        // The bookkeeping connection: the same one on a primary, the writer's on a
        // standby (§10cz). UTC on it too — cutoff_version::text is rendered in the
        // session timezone, and a manifest must carry canonical `+00` (livebirth §5).
        const bkc: *c.PGconn = if (self.book_config == self.pg_config) pgc else blk: {
            const bk_info = try self.book_config.connInfo(alloc, false);
            const bk = c.PQconnectdb(bk_info.ptr) orelse return error.ConnectionFailed;
            if (c.PQstatus(bk) != c.CONNECTION_OK) {
                log.err("🧬 PG connect failed (bookkeeping, DATABASE_WRITER_URL): {s}", .{c.PQerrorMessage(bk)});
                c.PQfinish(bk);
                return error.ConnectionFailed;
            }
            const tz = try queryOne(bk, "SET timezone TO 'UTC'", &.{});
            c.PQclear(tz);
            break :blk bk;
        };
        defer if (bkc != pgc) c.PQfinish(bkc);
        {
            const res = try queryOne(pgc, "SET timezone TO 'UTC'", &.{});
            c.PQclear(res);
        }

        var conn_nats = nats.Connection.init(self.allocator, self.io, .{
            .user = self.endpoint.user,
            .password = self.endpoint.pass,
            .nkey_seed = self.endpoint.seed,
            .user_creds = self.endpoint.creds,
        });
        defer conn_nats.deinit();
        const url = try std.fmt.allocPrint(alloc, "nats://{s}:{d}", .{ self.endpoint.host, self.endpoint.port });
        try conn_nats.connect(url);
        var js = conn_nats.jetstream(.{});

        // ── derive the pair list: the publication IS the list ────────────────
        // Minus internals (zebridge_is_internal_table — one predicate, every door),
        // minus keyless tables (the client's guarded upsert needs a key), minus
        // explicit opt-outs (zebridge_enable(generations => false)). Routability and
        // tenancy are decided per table below; GENERATION_RULES, when set, only
        // INTERSECTS what was derived.
        const pub_z = try alloc.dupeZ(u8, self.publication_name);
        const derive_params = [_]?[*:0]const u8{pub_z.ptr};
        // The catalogue rides the derive query: per-tick tenant/version columns and
        // the generations opt-out come from `zebridge_catalogue` (LEFT JOIN — a table
        // with no row yet falls back to the boot-time maps and defaults), which is
        // what makes chain onboarding fully LIVE: a table enabled mid-flight gets its
        // chain on the next tick with no restart and no env edit.
        const derived = try queryOne(pgc, "SELECT pt.tablename, COALESCE(cat.tenant_col::text, ''), COALESCE(cat.version_col::text, ''), " ++
            "COALESCE(cat.tombstone_col::text, ''), " ++
            // §10ek: the soft-delete guard (init.write, `zebridge_soft_delete_t`) turns a DELETE
            // into a tombstone and lets only the sweeper through — on such a table a moving
            // delete counter can only mean reaps, which every replica removed at the tombstone.
            "EXISTS (SELECT 1 FROM pg_trigger g JOIN pg_class gc ON gc.oid = g.tgrelid JOIN pg_namespace gn ON gn.oid = gc.relnamespace " ++
            "        WHERE gn.nspname = pt.schemaname AND gc.relname = pt.tablename AND g.tgname = 'zebridge_soft_delete_t') " ++
            "FROM pg_publication_tables pt " ++
            "LEFT JOIN public.zebridge_catalogue cat ON cat.tbl = pt.tablename " ++
            "WHERE pt.pubname = $1 " ++
            "AND COALESCE(cat.generations, true) " ++
            "AND NOT public.zebridge_is_internal_table(pt.tablename) " ++
            // Catalog joins, no regclass cast: resolving `format(...)::regclass`
            // as the READER touched pg_toast schema resolution and was refused —
            // pg_class/pg_namespace/pg_index answer the same question with plain
            // catalog reads any role may make.
            "AND EXISTS (SELECT 1 FROM pg_class cl " ++
            "            JOIN pg_namespace ns ON ns.oid = cl.relnamespace " ++
            "            JOIN pg_index i ON i.indrelid = cl.oid AND i.indisprimary " ++
            "            WHERE ns.nspname = pt.schemaname AND cl.relname = pt.tablename) " ++
            "ORDER BY 1", &derive_params);
        defer c.PQclear(derived);

        const restricted = self.rules.count() > 0;
        var pairs: usize = 0;
        const n_tables: usize = @intCast(c.PQntuples(derived));
        // The published set, for the departure sweep below (§10dg).
        var published_lit: std.ArrayList(u8) = .empty;
        try published_lit.append(alloc, '{');
        for (0..n_tables) |i| {
            if (i > 0) try published_lit.append(alloc, ',');
            try published_lit.append(alloc, '"');
            try published_lit.appendSlice(alloc, std.mem.span(c.PQgetvalue(derived, @intCast(i), 0)));
            try published_lit.append(alloc, '"');
        }
        try published_lit.append(alloc, '}');
        self.sweepDeparted(alloc, bkc, &js, published_lit.items) catch |err| {
            log.warn("🧬 departure sweep failed: {} — next cadence retries", .{err});
        };
        for (0..n_tables) |i| {
            if (self.should_stop.load(.acquire)) return;
            const table = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(derived, @intCast(i), 0)));
            const cat_tenant_col = std.mem.span(c.PQgetvalue(derived, @intCast(i), 1));
            const cat_version_col = std.mem.span(c.PQgetvalue(derived, @intCast(i), 2));
            const tcol = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(derived, @intCast(i), 3)));
            const guarded = c.PQgetvalue(derived, @intCast(i), 4)[0] == 't';

            // Catalogue first (live), boot-time maps as fallback (env overrides
            // were already merged into those maps at boot — env still wins there).
            const env_tenant_cols = self.tenant_rules.get(table);
            const tenant_col_eff: []const u8 =
                if (env_tenant_cols) |cols| cols[0] else cat_tenant_col;
            const tenant_scoped = tenant_col_eff.len > 0;
            // A table no client can follow over CDC gets no chain either: chains seed
            // what CDC then keeps current, and seeding the unfollowable is pure waste.
            if (!tenant_scoped and !self.topo.isCdcRoutable(table, false)) continue;

            const allowed: ?[]const []const u8 = if (restricted) self.rules.get(table) else null;
            if (restricted and allowed == null) continue;

            const vcol = blk: {
                if (self.sync_rules.get(table)) |cols| {
                    if (cols.len > 0 and cols[0].len > 0) break :blk cols[0];
                }
                break :blk if (cat_version_col.len > 0)
                    cat_version_col
                else
                    config.Sync.default_version_column;
            };

            if (tenant_scoped) {
                // The tenant SET comes from the data, not from grammar.json — the
                // dyntenant lesson: a new tenant's first row creates its chain on the
                // next tick, and a tenant with no rows has nothing to seed.
                const tbl_ref = try utils.allocPrintZ(alloc, "public.\"{s}\"", .{table});
                const col_z = try alloc.dupeZ(u8, tenant_col_eff);
                const tparams = [_]?[*:0]const u8{ tbl_ref.ptr, col_z.ptr };
                const tres = queryOne(pgc, "SELECT * FROM public.zebridge_tenants_of($1::regclass, $2::name)", &tparams) catch |err| {
                    log.err("🧬 tenants_of('{s}') failed: {} — table skipped this tick", .{ table, err });
                    continue;
                };
                defer c.PQclear(tres);
                const nt: usize = @intCast(c.PQntuples(tres));
                for (0..nt) |t| {
                    const tenant = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(tres, @intCast(t), 0)));
                    if (allowed) |a| {
                        var ok = false;
                        for (a) |cand| ok = ok or std.mem.eql(u8, cand, tenant);
                        if (!ok) continue;
                    }
                    pairs += 1;
                    self.buildWithRetry(alloc, pgc, bkc, &js, table, tenant, vcol, tcol, guarded);
                }
            } else {
                const tenant = self.topo.open_tenant;
                if (allowed) |a| {
                    var ok = false;
                    for (a) |cand| ok = ok or std.mem.eql(u8, cand, tenant);
                    if (!ok) continue;
                }
                pairs += 1;
                self.buildWithRetry(alloc, pgc, bkc, &js, table, tenant, vcol, tcol, guarded);
            }
        }
        log.debug("🧬 tick: {d} published table(s) derived, {d} (table, tenant) pair(s) built or checked", .{ n_tables, pairs });
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

    /// §10ep: the same document from `COPY (…) TO STDOUT (FORMAT binary)`, the rows
    /// decoded by the CDC path's own decoder (`pgoutput.decodeBinColumnData`).
    ///
    /// `pgoutput` in binary mode and binary COPY use the same per-type encoding —
    /// PostgreSQL's send functions — so a chain row arrives in the form the decoder
    /// reads on every CDC event of every day; what this adds is the COPY framing (a
    /// signature, a field count per row, a length per field) and the mapping to wire
    /// values that `batch_publisher.decodePackedValue` uses for events: integers and
    /// floats as numbers, everything else as strings. Two things follow. The text
    /// result's 1.5 µs a row of rendering and parsing (§10eo) goes; and a chain row
    /// and a CDC row of the same PostgreSQL value are now the same bytes on the wire,
    /// where a timestamp used to reach a client as PostgreSQL's text through the chain
    /// and as the decoder's ISO form through CDC. A type the decoder refuses fails the
    /// whole build over to the text path (a table with such a type is suspended on CDC
    /// anyway); the decision is per build and said in the log.
    fn encodeContentCopy(
        alloc: std.mem.Allocator,
        pgc: *c.PGconn,
        select_sql: []const u8,
        gen: i64,
        kind: []const u8,
        cutoff: []const u8,
        prev_cutoff: ?[]const u8,
        vcol: []const u8,
        out_rows: *usize,
        out_widest: *usize,
    ) ![]const u8 {
        // Names and OIDs, from the same SELECT under LIMIT 0 — inside the same
        // snapshot transaction, so the shape cannot differ from the rows that follow.
        const probe_sql = try utils.allocPrintZ(alloc, "SELECT * FROM ({s}) AS q LIMIT 0", .{select_sql});
        const meta = try queryOne(pgc, probe_sql, &.{});
        defer c.PQclear(meta);
        const ncols: usize = @intCast(c.PQnfields(meta));
        const names = try alloc.alloc([]const u8, ncols);
        const oids = try alloc.alloc(u32, ncols);
        var names_bytes: usize = 0;
        for (0..ncols) |i| {
            names[i] = try alloc.dupe(u8, std.mem.span(c.PQfname(meta, @intCast(i))));
            oids[i] = c.PQftype(meta, @intCast(i));
            names_bytes += names[i].len;
        }

        const copy_sql = try utils.allocPrintZ(alloc, "COPY ({s}) TO STDOUT (FORMAT binary)", .{select_sql});
        const started = c.PQexec(pgc, copy_sql.ptr) orelse return error.QueryFailed;
        if (c.PQresultStatus(started) != c.PGRES_COPY_OUT) {
            log.err("🧬 COPY refused: {s}", .{c.PQerrorMessage(pgc)});
            c.PQclear(started);
            return error.QueryFailed;
        }
        c.PQclear(started);

        var enc = encoder_mod.Encoder.init(alloc, .msgpack);
        var root = enc.createMap();
        var cols_arr = try enc.createArray(ncols);
        for (0..ncols) |i| try cols_arr.setIndex(i, try enc.createString(names[i]));
        try root.put(enc.allocator, "columns", cols_arr);

        var rows: std.ArrayListUnmanaged(encoder_mod.Value) = .empty;
        var header_seen = false;
        var failed: ?anyerror = null;
        copy: while (true) {
            var buf: [*c]u8 = undefined;
            const n = c.PQgetCopyData(pgc, &buf, 0);
            if (n == -1) break; // the server's end of COPY
            if (n < 0) {
                log.err("🧬 COPY read failed: {s}", .{c.PQerrorMessage(pgc)});
                failed = error.QueryFailed;
                break;
            }
            defer c.PQfreemem(buf);
            const data = buf[0..@intCast(n)];
            var pos: usize = 0;
            if (!header_seen) {
                // "PGCOPY\n\377\r\n\0", then int32 flags, then int32 extension length.
                if (data.len < 19 or !std.mem.eql(u8, data[0..11], "PGCOPY\n\xff\r\n\x00")) {
                    failed = error.CopyHeader;
                    break;
                }
                const ext_len: usize = @intCast(std.mem.readInt(i32, data[15..19], .big));
                pos = 19 + ext_len;
                header_seen = true;
            }
            while (pos + 2 <= data.len) {
                const nfields = std.mem.readInt(i16, data[pos..][0..2], .big);
                pos += 2;
                // The trailer: no more rows, but libpq is still in COPY state until it
                // has seen the server's CopyDone — keep reading until it says -1.
                if (nfields == -1) continue :copy;
                if (@as(usize, @intCast(nfields)) != ncols) {
                    failed = error.CopyShape;
                    break :copy;
                }
                var row_arr = try enc.createArray(ncols);
                var row_bytes: usize = names_bytes + 256; // envelope margin, mirrors wireSize
                for (0..ncols) |i| {
                    const flen = std.mem.readInt(i32, data[pos..][0..4], .big);
                    pos += 4;
                    if (flen == -1) {
                        try row_arr.setIndex(i, enc.createNull());
                        continue;
                    }
                    const len: usize = @intCast(flen);
                    const bytes = data[pos..][0..len];
                    pos += len;
                    row_bytes += len;
                    // `owns_bytes = false`: the decoder dupes what it keeps, since libpq's
                    // buffer is freed at the end of this message.
                    const v = pgoutput.decodeBinColumnData(alloc, oids[i], bytes, false) catch |err| {
                        log.warn("🧬 COPY: column {s} (oid {d}) is not decodable by the CDC decoder ({s}) — this build takes the text path", .{ names[i], oids[i], @errorName(err) });
                        failed = err;
                        break :copy;
                    };
                    try row_arr.setIndex(i, switch (v) {
                        .null, .unchanged => enc.createNull(),
                        .boolean => |b| enc.createBool(b),
                        .int32 => |x| enc.createInt(@intCast(x)),
                        .int64 => |x| enc.createInt(x),
                        .float64 => |f| enc.createFloat(f),
                        .text, .numeric, .jsonb, .array, .bytea => |str| try enc.createString(str),
                    });
                }
                if (row_bytes > out_widest.*) out_widest.* = row_bytes;
                try rows.append(alloc, row_arr);
            }
        }
        // Whatever ended the COPY, the connection must be left clean: read the rest
        // of the data until libpq reports the end (-1, or -2 on a broken copy) and
        // then collect the results, or the next statement of this transaction fails.
        // ⚠️ Unconditional and BOUNDED. Asking for results while libpq is still in
        // COPY state hands back a COPY_OUT result every time, for ever: the first
        // version looped there, the producer thread never returned, and the bridge's
        // graceful stop waited on it until it was killed (measured 2026-09-11).
        {
            var buf2: [*c]u8 = undefined;
            while (c.PQgetCopyData(pgc, &buf2, 0) > 0) c.PQfreemem(buf2);
        }
        var guard: u8 = 0;
        while (c.PQgetResult(pgc)) |r| : (guard += 1) {
            const st = c.PQresultStatus(r);
            c.PQclear(r);
            if (st == c.PGRES_COPY_OUT or guard >= 8) {
                log.err("🧬 COPY did not end cleanly (status {d} after {d} result(s)): {s}", .{ st, guard, c.PQerrorMessage(pgc) });
                failed = failed orelse error.QueryFailed;
                break;
            }
            if (failed == null and st != c.PGRES_COMMAND_OK) {
                log.err("🧬 COPY ended badly: {s}", .{c.PQerrorMessage(pgc)});
                failed = error.QueryFailed;
            }
        }
        if (failed) |e| return e;

        out_rows.* = rows.items.len;
        var rows_arr = try enc.createArray(rows.items.len);
        for (rows.items, 0..) |r, i| try rows_arr.setIndex(i, r);
        try root.put(enc.allocator, "rows", rows_arr);
        try root.put(enc.allocator, "gen", enc.createInt(gen));
        try root.put(enc.allocator, "kind", try enc.createString(kind));
        try root.put(enc.allocator, "cutoff", try enc.createString(cutoff));
        try root.put(enc.allocator, "version_column", try enc.createString(vcol));
        if (prev_cutoff) |p| try root.put(enc.allocator, "prev_cutoff", try enc.createString(p));
        return try enc.encode(root);
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
        out_widest: *usize,
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
        var names_bytes: usize = 0;
        for (0..ncols) |i| names_bytes += std.mem.span(c.PQfname(res, @intCast(i))).len;
        var rows_arr = try enc.createArray(nrows);
        for (0..nrows) |r| {
            var row_bytes: usize = names_bytes + 256; // envelope margin, mirrors wireSize
            var row_arr = try enc.createArray(ncols);
            for (0..ncols) |col| {
                if (c.PQgetisnull(res, @intCast(r), @intCast(col)) == 1) {
                    try row_arr.setIndex(col, enc.createNull());
                } else {
                    const val = std.mem.span(c.PQgetvalue(res, @intCast(r), @intCast(col)));
                    row_bytes += val.len;
                    // ⚠️ Text-mode results render a boolean as `t`/`f`, while CDC carries a
                    // real boolean. A SQLite replica stores the chain's `t` as TEXT (no
                    // numeric affinity rescues it) and the CDC echo's `true` as INTEGER 1 —
                    // the same column in two forms, and `WHERE is_true = 1` missed every
                    // chain-seeded row (measured 2026-08-29). Ints and floats are safe: their
                    // text is numeric and the column affinity converts. Booleans are the one
                    // type that must leave here as what CDC sends.
                    if (c.PQftype(res, @intCast(col)) == 16) { // BOOLOID
                        try row_arr.setIndex(col, enc.createBool(val.len > 0 and val[0] == 't'));
                    } else {
                        try row_arr.setIndex(col, try enc.createString(val));
                    }
                }
            }
            if (row_bytes > out_widest.*) out_widest.* = row_bytes;
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

    /// §10dg: a table that left the publication (dropped, or disabled) leaves its
    /// chain behind — bookkeeping rows, objects, and a manifest that still names a
    /// full built from the OLD shape. Measured: a table dropped and re-created under
    /// the same name inherited that chain, and the first client to seed asked its
    /// fresh table for a column of the old one. The bookkeeping is the authority:
    /// every (tenant, table) it holds that is not published now is swept — objects
    /// first, then the manifest, then the rows — so a re-created table starts at g1.
    fn sweepDeparted(self: *GenerationProducer, alloc: std.mem.Allocator, bkc: *c.PGconn, js: *nats.JetStream, published_lit: []const u8) !void {
        const lit_z = try alloc.dupeZ(u8, published_lit);
        const params = [_]?[*:0]const u8{lit_z.ptr};
        const gone = try queryOne(bkc, "SELECT DISTINCT tenant, tbl FROM public.zebridge_generations WHERE NOT (tbl = ANY($1::text[])) ORDER BY 1, 2", &params);
        defer c.PQclear(gone);
        const n: usize = @intCast(c.PQntuples(gone));
        for (0..n) |i| {
            const tenant = std.mem.span(c.PQgetvalue(gone, @intCast(i), 0));
            const table = std.mem.span(c.PQgetvalue(gone, @intCast(i), 1));
            try self.sweepPair(alloc, bkc, js, tenant, table);
            log.info("🧬 '{s}'/'{s}' left the publication — chain swept; a table reborn under this name starts at g1", .{ tenant, table });
        }
    }

    /// One (tenant, table) chain gone for good: its objects, its manifest, its rows.
    fn sweepPair(self: *GenerationProducer, alloc: std.mem.Allocator, bkc: *c.PGconn, js: *nats.JetStream, tenant: []const u8, table: []const u8) !void {
        const tenant_z = try alloc.dupeZ(u8, tenant);
        const table_z = try alloc.dupeZ(u8, table);
        const pair = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
        const bucket = try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.topo.generation_bucket_prefix, tenant });
        var osm = js.objectStoreManager();
        var deleted: usize = 0;
        if (osm.openStore(bucket)) |*store_v| {
            var store = store_v.*;
            defer store.deinit();
            const rows = try queryOne(bkc, "SELECT gen FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 ORDER BY gen", &pair);
            defer c.PQclear(rows);
            const ng: usize = @intCast(c.PQntuples(rows));
            for (0..ng) |k| {
                const g = std.mem.span(c.PQgetvalue(rows, @intCast(k), 0));
                for ([_][]const u8{ "delta", "full", "dict" }) |kind| {
                    const name = try std.fmt.allocPrint(alloc, "{s}-g{s}-{s}", .{ table, g, kind });
                    store.delete(name) catch |err| {
                        if (err != error.ObjectNotFound) log.warn("🧬 sweep: could not delete {s}: {}", .{ name, err });
                        continue;
                    };
                    deleted += 1;
                }
            }
        } else |_| {}
        var kv = try js.kvBucket(self.topo.kv_generations);
        defer kv.deinit();
        const key = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ tenant, table });
        kv.delete(key) catch |err| log.warn("🧬 sweep: could not delete manifest {s}: {}", .{ key, err });
        const del = try queryOne(bkc, "DELETE FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2", &pair);
        c.PQclear(del);
        log.debug("🧬 '{s}'/'{s}': {d} object(s), manifest and bookkeeping swept", .{ tenant, table, deleted });
    }

    fn buildOne(
        self: *GenerationProducer,
        alloc: std.mem.Allocator,
        pgc: *c.PGconn,
        bkc: *c.PGconn,
        js: *nats.JetStream,
        table: []const u8,
        tenant: []const u8,
        vcol: []const u8,
        /// The tombstone column ('' when the table keeps none): a full carries only live
        /// rows (§10ek), a delta carries the tombstoned ones — the delete signal.
        tcol: []const u8,
        /// The soft-delete guard is on the table: deletes are reaps, never a hole.
        guarded: bool,
    ) !void {
        const build_started_ms = utils.unixMillis();
        const table_z = try alloc.dupeZ(u8, table);
        const tenant_z = try alloc.dupeZ(u8, tenant);

        // ── last generation and last full, from the producer's own memory ────
        var last_gen: i64 = 0;
        var last_cutoff: ?[]const u8 = null;
        var last_full_gen: i64 = 0;
        // The row count at the last generation's cutoff — null on rows written
        // before the column existed, which reads as "unknown" and builds once.
        var last_row_count: ?i64 = null;
        // The table's cumulative delete count (pg_stat_user_tables.n_tup_del) at the
        // last cutoff — the one number a hard delete cannot hide from, even when
        // offset by an insert. Table-wide, not tenant-scoped: any tenant's delete
        // costs every tenant of that table one full, which is conservative and cheap.
        var last_del_count: ?i64 = null;
        var last_epoch: i64 = 0;
        var last_col_shape: ?[]const u8 = null;
        var last_relid: ?[]const u8 = null;
        {
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
            const res = try queryOne(bkc, "SELECT gen, cutoff_version::text, row_count, del_count, seed_epoch, col_shape, relid::text FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 ORDER BY gen DESC LIMIT 1", &params);
            defer c.PQclear(res);
            if (c.PQntuples(res) > 0) {
                last_gen = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch 0;
                last_cutoff = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 1)));
                if (c.PQgetisnull(res, 0, 2) == 0) {
                    last_row_count = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 2)), 10) catch null;
                }
                if (c.PQgetisnull(res, 0, 3) == 0) {
                    last_del_count = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 3)), 10) catch null;
                }
                last_epoch = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 4)), 10) catch 0;
                if (c.PQgetisnull(res, 0, 5) == 0) last_col_shape = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 5)));
                if (c.PQgetisnull(res, 0, 6) == 0) last_relid = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 6)));
            }
        }
        // §10dl: the table's OID now. Different from the last generation's → the table
        // was dropped and re-created under the same name within one tick — a different
        // table with the same chain name. Measured: two clients seeded the 40,001 rows
        // of the old incarnation into a table that had 30,000. The departure sweep
        // cannot see it (the name never left the publication), so it is done HERE:
        // old chain swept, epoch bumped (every replica re-seeds), chain restarted at g1.
        const cur_relid: []const u8 = blk: {
            const params = [_]?[*:0]const u8{table_z.ptr};
            const res = try queryOne(pgc, "SELECT COALESCE(to_regclass(format('%I.%I', 'public', $1::text))::oid::text, '')", &params);
            defer c.PQclear(res);
            break :blk try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0)));
        };
        if (last_gen != 0 and last_relid != null and cur_relid.len > 0 and !std.mem.eql(u8, last_relid.?, cur_relid)) {
            log.warn("🧬 '{s}'/'{s}': the table was re-created under its name (oid {s} -> {s}) since g{d} — sweeping the old chain, bumping the seed epoch, starting at g1", .{ tenant, table, last_relid.?, cur_relid, last_gen });
            self.sweepPair(alloc, bkc, js, tenant, table) catch |err| log.warn("🧬 sweep of the reborn table's chain failed: {}", .{err});
            {
                const params = [_]?[*:0]const u8{table_z.ptr};
                const res = queryOne(bkc, "SELECT count(*) FROM public.zebridge_reseed(to_regclass(format('%I.%I', 'public', $1::text)))", &params) catch null;
                if (res) |r| c.PQclear(r);
            }
            last_gen = 0;
            last_cutoff = null;
            last_row_count = null;
            last_del_count = null;
            last_epoch = 0;
            last_col_shape = null;
        }
        // §10df: the catalogue's seed_epoch now. Different from the one the last
        // generation was built under → zebridge_reseed() ran → a FULL, whatever the
        // counts and versions say (a re-key or a volatile default moves neither).
        const cat_epoch: i64 = blk: {
            const p = [_]?[*:0]const u8{table_z.ptr};
            const r = try queryOne(pgc, "SELECT seed_epoch FROM public.zebridge_catalogue WHERE tbl = $1", &p);
            defer c.PQclear(r);
            break :blk if (c.PQntuples(r) > 0) (std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(r, 0, 0)), 10) catch 0) else 0;
        };
        {
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
            const res = try queryOne(bkc, "SELECT gen FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND has_full ORDER BY gen DESC LIMIT 1", &params);
            defer c.PQclear(res);
            if (c.PQntuples(res) > 0) {
                last_full_gen = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch 0;
            }
        }

        // ── unchanged-by-version: the delta filter's own predicate, so passing it
        // means the delta will be non-empty. ⚠️ It is BLIND TO HARD DELETES: a row
        // that is gone has no version to be newer than the cutoff. On a table with a
        // tombstone column that is fine (a soft delete bumps the version and the reap
        // is never forwarded, PROTOCOL §7.5); on a table without one it left the last
        // full describing rows PostgreSQL no longer had — measured: `memo` 6 rows in
        // every fresh replica against 2 in PostgreSQL, with the DELETE events long
        // evicted from CDC_PUBLIC, so no client had a path back (NOTES §10bb). The
        // decision is therefore NOT taken here: it is completed inside the snapshot
        // below, where the row count can be compared under the same tenant scope.
        var unchanged_by_version = false;
        if (last_cutoff) |cut| {
            const cut_z = try alloc.dupeZ(u8, cut);
            const check = try utils.allocPrintZ(alloc, "SELECT EXISTS(SELECT 1 FROM \"{s}\" WHERE \"{s}\" > $1::timestamptz - interval '{s}')", .{ table, vcol, config.Sync.version_future_tolerance });
            const params = [_]?[*:0]const u8{cut_z.ptr};
            const res = try queryOne(pgc, check, &params);
            defer c.PQclear(res);
            unchanged_by_version = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, 0, 0)), "f");
        }

        const gen = last_gen + 1;
        // A full at gen 1 (nothing to delta against, and the chain needs its jump-in
        // point), then refreshed BEFORE the last one can age out of the kept window
        // (gen > N − depth): rebuild at distance depth − 1 keeps it always inside.
        const build_delta = last_gen > 0;
        var build_full = last_full_gen == 0 or (gen - last_full_gen) >= @as(i64, self.chain_depth) - 1;
        // §10df, decided HERE — before anything below builds or names the full. Placed
        // after the full-building blocks once, it only flipped the label: bookkeeping
        // and manifest claimed a full that was never written (measured, a client
        // fetched a tombstone).
        // §10dl: no bookkeeping for this pair, yet a manifest in the bucket — an orphan
        // from a previous incarnation whose rows were removed by hand (a scenario
        // teardown, an operator's cleanup). Measured: two clients seeded 40,001 rows of
        // the old table from it. Its objects go, the key goes, and the epoch moves so
        // any replica that drank from it re-seeds from this build.
        if (last_gen == 0) {
            const key0 = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ tenant, table });
            if (js.kvBucket(self.topo.kv_generations)) |*kv0_v| {
                var kv0 = kv0_v.*;
                defer kv0.deinit();
                if (kv0.get(key0)) |entry_v| {
                    var entry = entry_v;
                    defer entry.deinit();
                    {
                        if (!entry.isDeleted() and entry.value.len > 0) {
                            log.warn("🧬 '{s}'/'{s}': a chain manifest with no bookkeeping behind it — a previous incarnation's; sweeping it and bumping the seed epoch", .{ tenant, table });
                            const bucket0 = try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.topo.generation_bucket_prefix, tenant });
                            var osm0 = js.objectStoreManager();
                            if (osm0.openStore(bucket0)) |*store_v| {
                                var store0 = store_v.*;
                                defer store0.deinit();
                                if (std.json.parseFromSliceLeaky(std.json.Value, alloc, entry.value, .{})) |man| {
                                    if (man == .object) {
                                        if (man.object.get("full")) |f| if (f == .object) {
                                            if (f.object.get("object")) |o| if (o == .string) store0.delete(o.string) catch {};
                                            if (f.object.get("gen")) |g| if (g == .integer) {
                                                const dname = try std.fmt.allocPrint(alloc, "{s}-g{d}-dict", .{ table, g.integer });
                                                store0.delete(dname) catch {};
                                            };
                                        };
                                        if (man.object.get("deltas")) |ds| if (ds == .array) for (ds.array.items) |d| if (d == .object) {
                                            if (d.object.get("object")) |o| if (o == .string) store0.delete(o.string) catch {};
                                            if (d.object.get("dict")) |o| if (o == .string) store0.delete(o.string) catch {};
                                        };
                                    }
                                } else |_| {}
                            } else |_| {}
                            kv0.delete(key0) catch {};
                            const params0 = [_]?[*:0]const u8{table_z.ptr};
                            const r0 = queryOne(bkc, "SELECT count(*) FROM public.zebridge_reseed(to_regclass(format('%I.%I', 'public', $1::text)))", &params0) catch null;
                            if (r0) |r| c.PQclear(r);
                        }
                    }
                } else |_| {}
            } else |_| {}
        }

        const epoch_moved = last_gen != 0 and cat_epoch != last_epoch;
        if (epoch_moved) {
            log.info("🧬 '{s}'/'{s}': seed epoch moved ({d} -> {d}) since g{d} — zebridge_reseed(); forcing a full", .{ tenant, table, last_epoch, cat_epoch, last_gen });
            build_full = true;
        }
        // §10dg: the column shape (names + types, attnum order) now, against the one
        // the last generation was built from. A chain object names its columns, and a
        // full built before a DROP/RENAME/re-type asks the replica for a column it no
        // longer has — a fresh replica could not seed until the depth rotation. Any
        // shape move → a FULL, whatever the counts say. (An ADD COLUMN moves it too:
        // the old full would seed rows without the column; harmless, but the new full
        // carries PostgreSQL's values for it.)
        const col_shape: []const u8 = blk: {
            const params = [_]?[*:0]const u8{table_z.ptr};
            const res = try queryOne(pgc, "SELECT COALESCE(string_agg(attname || ':' || format_type(atttypid, atttypmod), ',' ORDER BY attnum), '') " ++
                "FROM pg_attribute WHERE attrelid = to_regclass('public.' || quote_ident($1)) AND attnum > 0 AND NOT attisdropped", &params);
            defer c.PQclear(res);
            break :blk try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0)));
        };
        const shape_moved = last_gen != 0 and last_col_shape != null and !std.mem.eql(u8, last_col_shape.?, col_shape);
        if (shape_moved) {
            log.info("🧬 '{s}'/'{s}': column shape moved since g{d} ({s} -> {s}) — forcing a full", .{ tenant, table, last_gen, last_col_shape.?, col_shape });
            build_full = true;
        }

        // ── 1. LSN BEFORE the snapshot (overlap-never-gap) ───────────────────
        const lsn: []const u8 = blk: {
            const res = try queryOne(pgc, "SELECT public.zebridge_wal_head()::text", &.{});
            defer c.PQclear(res);
            break :blk try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0)));
        };

        // ── 1b. the CDC stream's last_seq, ALSO before the snapshot (finding 7,
        // NOTES §10i). Stream sequence is commit-ordered where lsn is not: a
        // transaction open across this build writes LOW lsns, commits late, and the
        // client's lsn gate dropped its events as "already in the snapshot" when the
        // snapshot never saw it. Everything published at or before this seq belongs
        // to a transaction that committed before our REPEATABLE READ begins (the
        // bridge publishes post-commit decode), so the snapshot contains it — the
        // one boundary that is exact in the stream's own coordinates. Capturing
        // BEFORE the snapshot keeps the overlap-never-gap direction: a visible but
        // not-yet-published transaction lands above the cutoff and is applied twice,
        // which the idempotent upsert absorbs.
        //
        // 0 = unavailable (stream missing or NATS hiccup): the manifest then omits
        // the field and clients fall back to the legacy lsn gate — degraded, never
        // wrong-er than before this field existed.
        const cutoff_seq: u64, const cdc_stream: []const u8, const stream_first: u64 = blk: {
            const name = if (std.mem.eql(u8, tenant, self.topo.open_tenant))
                self.topo.cdc_stream_public
            else
                try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.topo.cdc_stream_prefix, tenant });
            if (js.getStreamInfo(name)) |info_const| {
                var info = info_const;
                defer info.deinit();
                break :blk .{ info.value.state.last_seq, try alloc.dupe(u8, name), info.value.state.first_seq };
            } else |err| {
                log.warn("🧬 '{s}'/'{s}': stream info for {s} failed ({}) — manifest ships without cutoff_seq, clients use the legacy lsn gate", .{ tenant, table, name, err });
                break :blk .{ 0, name, 0 };
            }
        };
        // §10ei: the chain must OVERLAP the stream. A client that fell off the stream
        // seeds from the newest manifest and resumes at its cutoff_seq; when the stream
        // no longer holds that sequence (a size valve, a purge, an age under the floor
        // of §10eg) the client says "predates the stream" and waits for the NEXT
        // generation — which an unchanged table would never get. So the previous
        // cutoff is checked against the stream's oldest message here, and a chain that
        // fell off forces a build even when nothing moved.
        const chain_fell_off: bool = blk: {
            if (last_gen == 0 or stream_first <= 1) break :blk false;
            var kvb = js.kvBucket(self.topo.kv_generations) catch break :blk false;
            defer kvb.deinit();
            const mkey = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ tenant, table });
            var entry = kvb.get(mkey) catch break :blk false;
            defer entry.deinit();
            const man = std.json.parseFromSliceLeaky(std.json.Value, alloc, entry.value, .{}) catch break :blk false;
            if (man != .object) break :blk false;
            const prev_cut: i64 = if (man.object.get("cutoff_seq")) |v| (if (v == .integer) v.integer else 0) else 0;
            break :blk prev_cut > 0 and prev_cut + 1 < @as(i64, @intCast(stream_first));
        };
        if (chain_fell_off) log.warn("🧬 '{s}'/'{s}': chain g{d} fell off {s} (its cutoff is below the stream's oldest message, seq {d}) — a returning client could not splice; cutting a delta with a fresh cut point", .{ tenant, table, last_gen, cdc_stream, stream_first });

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

        // ── the count-at-cutoff check (NOTES §10bb): inside the snapshot, under the
        // tenant scope, so it is the same count the content queries see ──────
        //
        // A hard delete is invisible to the version predicate but not to count(*).
        // Compared with the count recorded at the LAST cutoff: equal and unchanged by
        // version → nothing to build; different → rows went (or came and went), and
        // the only artifact that can tell a client about a missing row is a FULL, so
        // one is forced regardless of the chain-depth schedule. A null last count is a
        // row from before this column existed: unknown, so build once and record it.
        //
        // ⚠️ Known blind spot, accepted for this candidate: N inserts and N deletes
        // between two ticks leave the count unchanged — but then the inserts fail the
        // version predicate, a delta is built, and its rows are the survivors; only a
        // delete of a row NOT touched since the last cutoff, exactly offset by a new
        // row, escapes. The tombstone-column route has no such gap; this one is for
        // tables that do not have it.
        const row_count_now: i64 = blk: {
            const sql = try utils.allocPrintZ(alloc, "SELECT count(*) FROM \"{s}\"", .{table});
            const res = try queryOne(pgc, sql, &.{});
            defer c.PQclear(res);
            break :blk std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch 0;
        };
        // ⚠️ The count's blind spot, closed: N inserts exactly offset by N deletes of
        // rows untouched since the cutoff leave count(*) unchanged. The statistics
        // collector's n_tup_del is cumulative per table and only ever grows — if it
        // moved, something was deleted, whatever the count says. It lags a commit by
        // up to ~500 ms, far inside a cadence; a stats reset drops it to 0, which reads
        // as "moved" and costs one extra full. NULL for a table the collector has not
        // seen yet: treated as 0.
        const del_count_now: i64 = blk: {
            const res = try queryOne(pgc, "SELECT COALESCE(n_tup_del, 0) FROM pg_stat_user_tables WHERE schemaname = 'public' AND relname = $1", &.{table_z.ptr});
            defer c.PQclear(res);
            if (c.PQntuples(res) == 0) break :blk 0;
            break :blk std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch 0;
        };
        // §10ek: on a guarded table the counter can only move for the sweeper's reaps of
        // rows every replica removed when their tombstone arrived (the delta object built
        // at that tick carries it, immutably) — no full is owed. A shrinking count(*)
        // on such a table is explained by the same reaps, UNLESS the counter did not
        // move at all: then rows went without a delete (TRUNCATE), and a full must.
        const counter_moved = if (last_del_count) |prev| del_count_now != prev else false;
        const deletes_moved = counter_moved and !guarded;
        const shrink_explained = guarded and counter_moved;
        if (guarded and counter_moved) log.info("🧬 '{s}'/'{s}': deletes since g{d} ({d} -> {d}) are the sweeper's reaps — the guard tombstones every other DELETE; no full owed", .{ tenant, table, last_gen, last_del_count.?, del_count_now });
        var repair_only = false;
        if (unchanged_by_version) {
            if (last_row_count) |prev| if (last_del_count == null) {
                log.info("🧬 '{s}'/'{s}': no delete count recorded for g{d} — building once to record {d}", .{ tenant, table, last_gen, del_count_now });
            } else {
                // An epoch move is a change even when nothing else moved (§10df): the
                // whole point of zebridge_reseed() is a full for data CDC never carried.
                if ((prev == row_count_now or shrink_explained) and !deletes_moved and !epoch_moved and !shape_moved) {
                    if (!chain_fell_off) {
                        log.debug("🧬 '{s}'/'{s}': unchanged since g{d} ({d} rows, {d} deletes) — skipped", .{ tenant, table, last_gen, prev, del_count_now });
                        const rb = try queryOne(pgc, "ROLLBACK", &.{});
                        c.PQclear(rb);
                        return;
                    }
                    // §10ej: nothing moved but the chain fell off the stream — the repair
                    // is an EMPTY delta, a fresh cut point and no rows: one small object,
                    // zero rows on every returning client. A full here would rebuild a
                    // large idle table because the stream moved, on both sides. The
                    // depth clock still decides a full when one is due.
                    repair_only = true;
                } else if (epoch_moved) {
                    log.info("🧬 '{s}'/'{s}': no version moved since g{d} but the seed epoch did (§10df) — forcing a full", .{ tenant, table, last_gen });
                } else if (shape_moved) {
                    log.info("🧬 '{s}'/'{s}': no version moved since g{d} but the column shape did (§10dg) — forcing a full", .{ tenant, table, last_gen });
                } else if (prev != row_count_now) {
                    log.info("🧬 '{s}'/'{s}': no version moved since g{d} but the row count did ({d} -> {d}) — rows were deleted; forcing a full", .{ tenant, table, last_gen, prev, row_count_now });
                } else {
                    log.info("🧬 '{s}'/'{s}': no version moved and the count held since g{d}, but n_tup_del moved ({d} -> {d}) — deletes offset by inserts; forcing a full", .{ tenant, table, last_gen, last_del_count.?, del_count_now });
                }
            } else {
                log.info("🧬 '{s}'/'{s}': no row count recorded for g{d} — building once to record {d}", .{ tenant, table, last_gen, row_count_now });
            }
            if (!repair_only) build_full = true;
        } else if (deletes_moved) {
            // Changed by version AND something was deleted: the delta carries what
            // moved, a full is the only thing that can carry an absence.
            log.info("🧬 '{s}'/'{s}': n_tup_del moved since g{d} ({d} -> {d}) — forcing a full alongside the delta", .{ tenant, table, last_gen, last_del_count.?, del_count_now });
            build_full = true;
        } else if (last_row_count) |prev| if (row_count_now < prev and !shrink_explained) {
            // Changed by version AND shrunk: the delta will carry the survivors that
            // moved, but nothing can carry the rows that went — a full must.
            log.info("🧬 '{s}'/'{s}': row count shrank since g{d} ({d} -> {d}) — forcing a full alongside the delta", .{ tenant, table, last_gen, prev, row_count_now });
            build_full = true;
        };

        // ── 3. content: full and/or delta against the SAME snapshot ──────────
        var full_payload: ?[]const u8 = null;
        var full_rows: usize = 0;
        var widest_row: usize = 0;
        // §10eo: where a build's time goes, one line per generation — the number that
        // decides whether the producer's per-row cost is the query, the encode, the
        // compression or the upload, before anyone parallelises the wrong stage.
        var ph: struct { query: i64 = 0, train: i64 = 0, zstd: i64 = 0, upload: i64 = 0 } = .{};
        if (build_full) {
            // §10ek: a full carries LIVE rows only. Every client applies a full as a wipe
            // and a reload inside one transaction, so a row absent from it is gone on the
            // replica whether or not the full named it; a tombstoned row in a full only
            // told the client to delete what it had already deleted. The fast sting's
            // full was 15 MiB for four live rows. The DELTA keeps its tombstoned rows:
            // that is the delete signal for a client catching up.
            const sql = if (tcol.len > 0)
                try utils.allocPrintZ(alloc, "SELECT * FROM \"{s}\" WHERE \"{s}\" IS NULL", .{ table, tcol })
            else
                try utils.allocPrintZ(alloc, "SELECT * FROM \"{s}\"", .{table});
            const t_q = utils.unixMillis();
            full_payload = encodeContentCopy(alloc, pgc, sql, gen, "full", cutoff_version, null, vcol, &full_rows, &widest_row) catch blk: {
                const res = try queryOne(pgc, sql, &.{});
                defer c.PQclear(res);
                break :blk try encodeContent(alloc, res, gen, "full", cutoff_version, null, vcol, &full_rows, &widest_row);
            };
            ph.query += utils.unixMillis() - t_q;
        }
        var delta_payload: ?[]const u8 = null;
        var delta_rows: usize = 0;
        if (build_delta) {
            // COPY takes no parameters: the previous cutoff is inlined as a literal. It is
            // PostgreSQL's own rendering of a timestamptz read back from the bookkeeping
            // row, quoted defensively all the same.
            const prev_lit = try std.mem.replaceOwned(u8, alloc, last_cutoff.?, "'", "''");
            const sql = try utils.allocPrintZ(alloc, "SELECT * FROM \"{s}\" WHERE \"{s}\" > '{s}'::timestamptz - interval '{s}'", .{ table, vcol, prev_lit, config.Sync.version_future_tolerance });
            const t_q = utils.unixMillis();
            delta_payload = encodeContentCopy(alloc, pgc, sql, gen, "delta", cutoff_version, last_cutoff, vcol, &delta_rows, &widest_row) catch blk: {
                const res = try queryOne(pgc, sql, &.{});
                defer c.PQclear(res);
                break :blk try encodeContent(alloc, res, gen, "delta", cutoff_version, last_cutoff, vcol, &delta_rows, &widest_row);
            };
            ph.query += utils.unixMillis() - t_q;
        }
        {
            const res = try queryOne(pgc, "COMMIT", &.{});
            c.PQclear(res);
        }

        // The retirement survivor of measureWidestRow (NOTES.md §1.13): the chain
        // carries any width (object chunking), but CDC cannot — a row at or over the
        // event buffer will suspend this table the moment ANY writer touches it. Say
        // so on every build, before it happens; the width-guard trigger stops NEW
        // rows, this catches the legacy ones already seeded to clients.
        if (widest_row >= self.event_buf_bytes) {
            log.warn("⚠️ '{s}'/'{s}': widest row ~{d} bytes is at or over the {d}-byte CDC event buffer. Chains carry it; CDC will SUSPEND this table on its next touch. Shrink the row (store a reference, not the blob) or raise BASE_BUF.", .{ tenant, table, widest_row, self.event_buf_bytes });
        }

        // ── 3b. the dictionary (§10x): a chain member, trained ONLY at a full ──
        // build from the full's own msgpack bytes. The producer's memory is PG:
        // the dictionary persists on the full's bookkeeping row and is read back
        // from THERE for the era's deltas — never from NATS. No training state
        // crosses eras: the next full trains a fresh one.
        var dict_bytes: ?[]u8 = null;
        var dict_name: ?[]const u8 = null;
        var dict_kept = false;
        var dict_ratio: ?i64 = null; // percent, on row-sized samples: the dictionary's own baseline
        const t_d = utils.unixMillis();
        if (build_full) {
            if (full_payload) |p| {
                // §10ea: the previous era's dictionary is KEPT while it still compresses
                // row-sized samples of this full the way it did when it was fresh — within
                // 10 points of the ratio recorded at its training. Drift is measured
                // against the dictionary's OWN past, not against "no dictionary": rows
                // that compress well on their own (the wasp's) made the latter test
                // retrain on a seven-point difference. Training is the expensive step;
                // immutable by name, a kept dictionary is simply named by the new era's
                // deltas. A shape change forces a full, and a drifted one is retrained.
                //
                // The probe compresses samples ONE BY ONE and sums: a dictionary earns its
                // keep on small inputs (a row, a few-KB delta), where zstd has no window to
                // find repetition in. A single contiguous megabyte finds it by itself and
                // made the dictionary look useless — retrained at every full for a whole
                // afternoon before this was measured.
                const probe = try strideCorpus(alloc, p, 128 * 1024);
                const params_p = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
                // ⚠️ The dictionary's NAME is the row's `dict_object`, never rebuilt from
                // the row's gen: a full that KEPT a dictionary stores the bytes under its
                // own gen with the older name, and rebuilding "g<gen>-dict" from that row
                // named an object nobody uploaded — every delta of two eras pointed at
                // a phantom, and every fresh client failed to seed (§10ed).
                const res_p = try queryOne(bkc, "SELECT gen, encode(dict, 'hex'), coalesce(dict_ratio::text, ''), coalesce(dict_object, '') FROM public.zebridge_generations " ++
                    "WHERE tenant=$1 AND tbl=$2 AND has_full AND dict IS NOT NULL ORDER BY gen DESC LIMIT 1", &params_p);
                defer c.PQclear(res_p);
                if (c.PQntuples(res_p) > 0 and probe.sizes.len > 0 and c.PQgetlength(res_p, 0, 3) > 0) {
                    const old = try hexDecode(alloc, std.mem.span(c.PQgetvalue(res_p, 0, 1)));
                    const old_gen = std.mem.span(c.PQgetvalue(res_p, 0, 0));
                    const old_name = std.mem.span(c.PQgetvalue(res_p, 0, 3));
                    const baseline: ?i64 = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res_p, 0, 2)), 10) catch null;
                    const now_pct = try probeRatio(alloc, probe, old);
                    // No baseline (a dictionary from before this column): a quarter smaller
                    // than no dictionary is the fallback test.
                    const keep = if (baseline) |b| now_pct <= b + 10 else blk: {
                        const without = try probeRatio(alloc, probe, null);
                        break :blk now_pct * 4 <= without * 3;
                    };
                    if (keep) {
                        dict_bytes = old;
                        dict_name = try alloc.dupe(u8, old_name);
                        dict_kept = true;
                        dict_ratio = baseline orelse now_pct;
                        log.info("📖 '{s}'/'{s}': g{d} keeps {s} — {d} row-sized samples compress to {d}% (its baseline {d}%)", .{ tenant, table, gen, dict_name.?, probe.sizes.len, now_pct, dict_ratio.? });
                    } else {
                        log.info("📖 '{s}'/'{s}': g{d} retrains — {s} compresses {d} row-sized samples to {d}% against its baseline {s}%: it drifted", .{ tenant, table, gen, old_gen, probe.sizes.len, now_pct, if (baseline) |b| try std.fmt.allocPrint(alloc, "{d}", .{b}) else "(none)" });
                    }
                }
                if (dict_bytes == null) if (try trainDict(alloc, p)) |d| {
                    dict_bytes = d;
                    dict_name = try std.fmt.allocPrint(alloc, "{s}-g{d}-dict", .{ table, gen });
                    dict_ratio = if (probe.sizes.len > 0) try probeRatio(alloc, probe, d) else null;
                };
            }
        } else if (build_delta) {
            const params_d = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
            // The name is the row's `dict_object` (see the probe above, §10ed).
            const res_d = try queryOne(bkc, "SELECT gen, encode(dict, 'hex'), coalesce(dict_object, '') FROM public.zebridge_generations " ++
                "WHERE tenant=$1 AND tbl=$2 AND has_full AND dict IS NOT NULL ORDER BY gen DESC LIMIT 1", &params_d);
            defer c.PQclear(res_d);
            if (c.PQntuples(res_d) > 0 and c.PQgetlength(res_d, 0, 2) > 0) {
                dict_bytes = try hexDecode(alloc, std.mem.span(c.PQgetvalue(res_d, 0, 1)));
                dict_name = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res_d, 0, 2)));
            }
        }

        // ── 4. immutable objects first ───────────────────────────────────────
        const bucket = try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.topo.generation_bucket_prefix, tenant });
        var osm = js.objectStoreManager();
        ph.train = utils.unixMillis() - t_d;
        var store = osm.openStore(bucket) catch |err| blk: {
            if (err == error.StoreNotFound or err == error.StreamNotFound) {
                break :blk try osm.createStore(.{ .store_name = bucket, .description = "ZeBridge generations (NOTES.md §1.13)" });
            }
            return err;
        };
        defer store.deinit();
        // Chain objects ship as zstd frames (§10w): built once, read by every
        // client forever — the one payload where compression amortizes fully.
        // Clients detect by the standard 4-byte magic, so mixed chains (older
        // uncompressed deltas referenced by the same manifest) keep working and
        // no object is ever rewritten. Fulls took level 9 as "read-many"; measured on a
        // 75,000-row full (§10eo) level 9 was 138 ms of a 327 ms build, 1.8 µs a row —
        // the producer's single largest cost, and the producer is the side that races
        // the stream (§10ej). Level 3 here, like the deltas; the ratio is compared below.
        if (full_payload) |p| {
            const t_z = utils.unixMillis();
            const z = try compressZstd(alloc, p, 3);
            ph.zstd += utils.unixMillis() - t_z;
            log.info("🗜️ '{s}'/'{s}': g{d} full {d} -> {d} bytes ({d}%)", .{ tenant, table, gen, p.len, z.len, z.len * 100 / @max(p.len, 1) });
            const name = try std.fmt.allocPrint(alloc, "{s}-g{d}-full", .{ table, gen });
            const t_u = utils.unixMillis();
            var r = try store.putBytes(name, z);
            r.deinit();
            ph.upload += utils.unixMillis() - t_u;
        }
        if (build_full and !dict_kept) if (dict_bytes) |d| {
            var r = try store.putBytes(dict_name.?, d);
            r.deinit();
            log.info("📖 '{s}'/'{s}': g{d} dictionary {d} bytes trained from a bounded sample of the full", .{ tenant, table, gen, d.len });
        };
        if (delta_payload) |p| {
            const t_z = utils.unixMillis();
            const z = if (dict_bytes) |d| try compressZstdDict(alloc, p, d, 3) else try compressZstd(alloc, p, 3);
            ph.zstd += utils.unixMillis() - t_z;
            log.info("🗜️ '{s}'/'{s}': g{d} delta {d} -> {d} bytes ({d}%){s}", .{ tenant, table, gen, p.len, z.len, z.len * 100 / @max(p.len, 1), if (dict_bytes != null) " [dict]" else "" });
            const name = try std.fmt.allocPrint(alloc, "{s}-g{d}-delta", .{ table, gen });
            const t_u = utils.unixMillis();
            var r = try store.putBytes(name, z);
            r.deinit();
            ph.upload += utils.unixMillis() - t_u;
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
            const res = try queryOne(bkc, "SELECT gen, cutoff_version::text, COALESCE(prev_cutoff::text, ''), has_full, COALESCE(dict_object, '') " ++
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
                    const dref = std.mem.span(c.PQgetvalue(res, @intCast(i), 4));
                    const dict_frag = if (dref.len > 0) try std.fmt.allocPrint(alloc, ",\"dict\":\"{s}\"", .{dref}) else "";
                    const frag = try std.fmt.allocPrint(alloc, "{s}{{\"gen\":{s},\"object\":\"{s}-g{s}-delta\",\"prev_cutoff\":\"{s}\",\"cutoff\":\"{s}\"{s}}}", .{ if (deltas_json.items.len > 0) "," else "", g, table, g, prev, cutoff, dict_frag });
                    try deltas_json.appendSlice(alloc, frag);
                }
            }
        }
        if (build_full) {
            full_gen_m = gen;
            full_cutoff_m = cutoff_version;
        }
        if (build_delta) {
            const dict_frag = if (dict_name) |dn| try std.fmt.allocPrint(alloc, ",\"dict\":\"{s}\"", .{dn}) else "";
            const frag = try std.fmt.allocPrint(alloc, "{s}{{\"gen\":{d},\"object\":\"{s}-g{d}-delta\",\"prev_cutoff\":\"{s}\",\"cutoff\":\"{s}\"{s}}}", .{ if (deltas_json.items.len > 0) "," else "", gen, table, gen, last_cutoff.?, cutoff_version, dict_frag });
            try deltas_json.appendSlice(alloc, frag);
        }

        var kv = js.kvBucket(self.topo.kv_generations) catch blk: {
            var km = js.kvManager();
            break :blk try km.createBucket(.{ .bucket = self.topo.kv_generations, .history = 1 });
        };
        defer kv.deinit();
        const key = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ tenant, table });
        const seq_frag: []const u8 = if (cutoff_seq > 0)
            try std.fmt.allocPrint(alloc, "\"cutoff_seq\":{d},\"cdc_stream\":\"{s}\",", .{ cutoff_seq, cdc_stream })
        else
            "";
        const manifest = try std.fmt.allocPrint(alloc, "{{\"gen\":{d},\"seed_epoch\":{d},\"bucket\":\"{s}\",{s}\"cutoff_version\":\"{s}\",\"cutoff_lsn\":\"{s}\"," ++
            "\"version_column\":\"{s}\"," ++
            "\"full\":{{\"gen\":{d},\"object\":\"{s}-g{d}-full\",\"cutoff\":\"{s}\"}},\"deltas\":[{s}]}}", .{ gen, cat_epoch, bucket, seq_frag, cutoff_version, lsn, vcol, full_gen_m, table, full_gen_m, full_cutoff_m, deltas_json.items });
        _ = try kv.put(key, manifest, .{});

        // ── objects and manifest live: NOW the row becomes the producer's memory ──
        {
            const gen_str = try utils.allocPrintZ(alloc, "{d}", .{gen});
            const lsn_z = try alloc.dupeZ(u8, lsn);
            const cut_z = try alloc.dupeZ(u8, cutoff_version);
            const prev_z: ?[*:0]const u8 = if (last_cutoff) |p| (try alloc.dupeZ(u8, p)).ptr else null;
            const dict_hex_z: ?[*:0]const u8 = if (build_full) (if (dict_bytes) |d| (try hexEncodeZ(alloc, d)).ptr else null) else null;
            const dict_obj_z: ?[*:0]const u8 = if (dict_name) |dn| (try alloc.dupeZ(u8, dn)).ptr else null;
            const count_z = try utils.allocPrintZ(alloc, "{d}", .{row_count_now});
            const del_z = try utils.allocPrintZ(alloc, "{d}", .{del_count_now});
            const epoch_z = try utils.allocPrintZ(alloc, "{d}", .{cat_epoch});
            const shape_z = try alloc.dupeZ(u8, col_shape);
            const relid_z: ?[*:0]const u8 = if (cur_relid.len > 0) (try alloc.dupeZ(u8, cur_relid)).ptr else null;
            const ratio_z: ?[*:0]const u8 = if (build_full) (if (dict_ratio) |r| (try utils.allocPrintZ(alloc, "{d}", .{r})).ptr else null) else null;
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, gen_str.ptr, cut_z.ptr, lsn_z.ptr, prev_z, if (build_full) "t" else "f", dict_hex_z, dict_obj_z, count_z.ptr, del_z.ptr, epoch_z.ptr, shape_z.ptr, relid_z, ratio_z };
            const res = try queryOne(bkc, "INSERT INTO public.zebridge_generations (tenant, tbl, gen, cutoff_version, cutoff_lsn, prev_cutoff, has_full, dict, dict_object, row_count, del_count, seed_epoch, col_shape, relid, dict_ratio) " ++
                "VALUES ($1, $2, $3, $4::timestamptz, $5::pg_lsn, $6::timestamptz, $7::boolean, decode($8, 'hex'), $9, $10::bigint, $11::bigint, $12::integer, $13, $14::oid, $15::smallint) " ++
                "ON CONFLICT (tenant, tbl, gen) DO NOTHING", &params);
            c.PQclear(res);
        }

        // ── 6. prune past the chain depth: PG rows (authority), then objects ──
        if (gen > self.chain_depth) {
            const keep_from = try utils.allocPrintZ(alloc, "{d}", .{gen - @as(i64, self.chain_depth)});
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, keep_from.ptr };
            const res = try queryOne(bkc, "DELETE FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND gen <= $3 RETURNING gen", &params);
            defer c.PQclear(res);
            const pruned: usize = @intCast(c.PQntuples(res));
            // Dictionaries outlive their full's row: a pruned era's dictionary must
            // survive while any REMAINING row was compressed with it (§10x).
            // ⚠️ Its OWN two parameters. This reused the DELETE's three-element array
            // above for a two-placeholder statement, and libpq refuses that: "bind
            // message supplies 3 parameters, but prepared statement requires 2". Latent
            // since §10x landed, because it only runs when a chain is deeper than
            // `chain_depth` — the first such build after it (memo g9, 2026-08-29)
            // failed here, after its objects and manifest were already live.
            const ref_params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
            const still_ref = try queryOne(bkc, "SELECT DISTINCT dict_object FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND dict_object IS NOT NULL", &ref_params);
            defer c.PQclear(still_ref);
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
                const dict_old = try std.fmt.allocPrint(alloc, "{s}-g{s}-dict", .{ table, g });
                var referenced = false;
                const nref: usize = @intCast(c.PQntuples(still_ref));
                for (0..nref) |k| {
                    if (std.mem.eql(u8, std.mem.span(c.PQgetvalue(still_ref, @intCast(k), 0)), dict_old)) referenced = true;
                }
                if (!referenced) store.delete(dict_old) catch |err| {
                    if (err != error.ObjectNotFound) log.warn("🧬 could not delete pruned dictionary {s}: {}", .{ dict_old, err });
                };
            }
        }

        // The duration is the number the retention contract needs (§10em): the CDC
        // window must cover two cadences AND this, since the cut is taken before the
        // build and must still be in the stream when the manifest is live.
        log.info("🧬 g{d} for '{s}'/'{s}': {s}{s}{s} → {s} (cutoff {s} @ {s}) in {d} ms", .{
            gen,                              tenant,                                      table,
            if (build_delta) "delta" else "", if (build_delta and build_full) "+" else "", if (build_full) "full" else "",
            bucket,                           cutoff_version,                              lsn,
            utils.unixMillis() - build_started_ms,
        });
        log.info("🧬   phases: copy+decode+encode {d} ms, dictionary {d} ms, zstd {d} ms, upload {d} ms — {d} full row(s), {d} delta row(s)", .{ ph.query, ph.train, ph.zstd, ph.upload, full_rows, delta_rows });
        if (build_delta) log.debug("🧬   delta: {d} row(s), {d} bytes", .{ delta_rows, delta_payload.?.len });
        if (build_full) log.debug("🧬   full:  {d} row(s), {d} bytes", .{ full_rows, full_payload.?.len });

        // §10ej: verify AFTER publishing that the cut is still inside the stream. The
        // cut is taken before the snapshot and the build takes time; under the burst
        // that pushed the stream past the chain in the first place, the stream's
        // oldest message can pass the new cut before the manifest is live — and then
        // no client can splice on it. Said here, retried at once by the tick (bounded).
        if (cutoff_seq > 0) {
            if (js.getStreamInfo(cdc_stream)) |info_c2| {
                var info2 = info_c2;
                defer info2.deinit();
                const first_now = info2.value.state.first_seq;
                if (cutoff_seq + 1 < first_now) {
                    self.cut_fell_off = true;
                    log.warn("🧬 '{s}'/'{s}': g{d}'s cut (seq {d}) fell off {s} during the build — the stream's oldest message is {d} now, the build took {d} ms; no client can splice on this generation", .{ tenant, table, gen, cutoff_seq, cdc_stream, first_now, utils.unixMillis() - build_started_ms });
                }
            } else |_| {}
        }
    }

    /// §10ej: one pair's build with the bounded retry the post-publish check asks
    /// for: the cut fell off during the build → build again at once, three times at
    /// most. Beyond that the stream prunes faster than this pair builds — a size
    /// valve under a burst, or an age under a build — and no retry can win: say so
    /// and leave it to the next tick; clients wait for a splice rather than read past
    /// the hole (§10ei). The back-pressure that would let the WAL absorb the burst
    /// (pause publishing for one build) is the next step, not this one.
    fn buildWithRetry(self: *GenerationProducer, alloc: std.mem.Allocator, pgc: *c.PGconn, bkc: *c.PGconn, js: *nats.JetStream, table: []const u8, tenant: []const u8, vcol: []const u8, tcol: []const u8, guarded: bool) void {
        var attempt: u8 = 0;
        while (attempt < 3) : (attempt += 1) {
            self.cut_fell_off = false;
            self.buildOne(alloc, pgc, bkc, js, table, tenant, vcol, tcol, guarded) catch |err| {
                log.err("🧬 generation build failed for '{s}'/'{s}': {} — next cadence retries", .{ tenant, table, err });
                return;
            };
            if (!self.cut_fell_off) return;
            if (attempt < 2) log.warn("🧬 '{s}'/'{s}': rebuilding at once ({d}/3)", .{ tenant, table, attempt + 2 });
        }
        log.err("🧬 '{s}'/'{s}': the cut fell off the stream three builds in a row — the stream prunes faster than this pair builds (CDC_MAX_BYTES / CDC_MAX_MSGS under a burst, or CDC_MAX_AGE_SECONDS under a build); clients wait for a splice until the next cadence", .{ tenant, table });
    }
};

fn compressZstd(alloc: std.mem.Allocator, src: []const u8, level: c_int) ![]u8 {
    const bound = c.ZSTD_compressBound(src.len);
    const dst = try alloc.alloc(u8, bound);
    const n = c.ZSTD_compress(dst.ptr, bound, src.ptr, src.len, level);
    if (c.ZSTD_isError(n) != 0) return error.ZstdCompressFailed;
    return dst[0..n];
}

/// §10x: train a zstd dictionary from one full's msgpack bytes, split into
/// delta-sized samples (2 KiB — the unit a dictionary will actually compress).
/// Null when the full is too small to learn from: dictionaries earn nothing
/// there, and zdict refuses tiny corpora anyway.
/// A bounded corpus: up to `max_bytes` of 2 KiB samples spread EVENLY over the full,
/// contiguous in one buffer (zdict wants that). Sampling instead of the whole full
/// keeps training — and the compression probe below — flat in CPU and memory
/// whatever the table's size; zstd's own guidance is ~100× the dictionary as corpus,
/// which 8 MiB is for a 112 KiB dictionary (§10ea).
fn strideCorpus(alloc: std.mem.Allocator, full: []const u8, max_bytes: usize) !struct { buf: []u8, sizes: []usize } {
    const sample_len: usize = 2048;
    const available = full.len / sample_len;
    const want = @min(available, max_bytes / sample_len);
    const buf = try alloc.alloc(u8, want * sample_len);
    const sizes = try alloc.alloc(usize, want);
    const stride = if (want > 0) available / want else 1;
    for (0..want) |i| {
        const off = i * stride * sample_len;
        @memcpy(buf[i * sample_len ..][0..sample_len], full[off..][0..sample_len]);
        sizes[i] = sample_len;
    }
    return .{ .buf = buf, .sizes = sizes };
}

/// Percent the row-sized samples compress to, one by one, with `dict` (or without).
fn probeRatio(alloc: std.mem.Allocator, probe: anytype, dict: ?[]const u8) !i64 {
    var total: usize = 0;
    var off: usize = 0;
    for (probe.sizes) |sz| {
        const sample = probe.buf[off .. off + sz];
        total += if (dict) |d| (try compressZstdDict(alloc, sample, d, 3)).len else (try compressZstd(alloc, sample, 3)).len;
        off += sz;
    }
    return @intCast(total * 100 / @max(probe.buf.len, 1));
}

fn trainDict(alloc: std.mem.Allocator, full: []const u8) !?[]u8 {
    const sample_len: usize = 2048;
    if (full.len < 16 * 1024 or full.len / sample_len < 8) return null;
    const corpus = try strideCorpus(alloc, full, 8 * 1024 * 1024);
    const nsamples = corpus.sizes.len;
    const cap: usize = @max(@as(usize, 1024), @min(@as(usize, 112 * 1024), corpus.buf.len / 4));
    const dict = try alloc.alloc(u8, cap);
    const n = c.ZDICT_trainFromBuffer(dict.ptr, cap, corpus.buf.ptr, corpus.sizes.ptr, @intCast(nsamples));
    if (c.ZDICT_isError(n) != 0) {
        log.warn("📖 dictionary training failed: {s}", .{std.mem.span(c.ZDICT_getErrorName(n))});
        return null;
    }
    return dict[0..n];
}

fn compressZstdDict(alloc: std.mem.Allocator, src: []const u8, dict: []const u8, level: c_int) ![]u8 {
    const cctx = c.ZSTD_createCCtx() orelse return error.ZstdCompressFailed;
    defer _ = c.ZSTD_freeCCtx(cctx);
    const bound = c.ZSTD_compressBound(src.len);
    const dst = try alloc.alloc(u8, bound);
    const n = c.ZSTD_compress_usingDict(cctx, dst.ptr, bound, src.ptr, src.len, dict.ptr, dict.len, level);
    if (c.ZSTD_isError(n) != 0) return error.ZstdCompressFailed;
    return dst[0..n];
}

fn hexEncodeZ(alloc: std.mem.Allocator, bytes: []const u8) ![:0]u8 {
    const out = try alloc.allocSentinel(u8, bytes.len * 2, 0);
    for (bytes, 0..) |b, i| utils.byteToHex(out[i * 2 .. i * 2 + 2], b);
    return out;
}

fn hexDecode(alloc: std.mem.Allocator, hex: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, hex.len / 2);
    for (out, 0..) |*b, i| b.* = try std.fmt.parseInt(u8, hex[i * 2 .. i * 2 + 2], 16);
    return out;
}
