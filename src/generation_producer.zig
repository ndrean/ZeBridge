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
const nats_endpoint = @import("nats_endpoint.zig");
const writable_tables = @import("writable_tables.zig");
const topology_mod = @import("topology.zig");
const c_imports = @import("c_imports.zig");
const hot_streams = @import("hot_streams.zig");
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
    /// §10eq: the newest cut of every pair this producer built, with what a rebuild of
    /// that pair alone needs. Written by the builds — on worker threads when
    /// GENERATION_WORKERS > 1 (§10ev) — and read by the edge watch on the producer's
    /// thread: `cuts_lock` guards the map, held for a lookup. Keys and strings are
    /// owned by `allocator` and live until deinit, so a copy of a Cut stays valid.
    cuts: std.StringArrayHashMapUnmanaged(Cut) = .empty,
    cuts_lock: utils.SpinLock = .{},
    /// §10ev: builders per tick (GENERATION_WORKERS). One builds on the tick's own
    /// thread and connections; more spawn that many threads, each with its own
    /// connections, taking pairs off a shared counter.
    workers: u32 = 1,
    /// Per CDC stream: `first_seq` at the previous edge check and when it was read —
    /// the prune rate is the difference over the interval.
    edges: std.StringArrayHashMapUnmanaged(Edge) = .empty,
    /// §10er: the full scan of every cut stream, a quarter of the cadence apart (five
    /// seconds at least) — the safety net; the streams that burst are read within a
    /// second, on the publisher's mark.
    edge_scan_seconds: u64 = 5,
    hot: ?*hot_streams.HotStreams = null,
    /// §10ge: GENERATION_DEFER_FULLS.
    defer_fulls: bool = true,
    /// §10gf: GENERATION_ASYNC_FULLS, and the lane's state. `cuts_lock` guards the three
    /// maps and the queue. `pair_busy`: a build of the pair holds it (every delta build
    /// whole; the full lane only while it takes its snapshot and while it attaches).
    async_fulls: bool = true,
    /// §10gq: how long a retired generation's objects survive in the store — longer than
    /// the slowest client's seed of a full, since a seed holds the object open while it
    /// applies. The bridge knows its build time, not a phone's apply speed: a setting.
    retire_grace_s: u64 = config.Generations.default_retire_grace_seconds,
    /// §10gq: how many retirements are kept at most, whatever the grace says — the storage
    /// bound, in fulls. The disk filled without it.
    retire_windows: u32 = config.Generations.default_retire_windows,
    /// §10gt: seconds between checkpoints; 0 turns the middle level off.
    checkpoint_s: u64 = config.Generations.default_checkpoint_seconds,
    /// §10gw: the base is rebuilt when the checkpoints since it weigh this much of it.
    base_rebuild_percent: u32 = config.Generations.default_base_rebuild_percent,
    /// §10gl: preflight's edge-writability verdicts. A table the edge writes keeps the
    /// fixed version tolerance under its delta floor: a client's version can trail the
    /// database clock by that much. Null (tests), or a table it does not know: kept too.
    writable: ?*const writable_tables.Registry = null,
    floor_warned: std.atomic.Value(bool) = .init(false),
    pair_busy: std.StringArrayHashMapUnmanaged(bool) = .empty,
    full_pending: std.StringArrayHashMapUnmanaged(void) = .empty,
    full_queue: std.ArrayListUnmanaged(FullJob) = .empty,
    full_thread: ?std.Thread = null,
    /// The CDC per-event buffer (2^BASE_BUF). The chain has no per-row ceiling —
    /// object chunking removes it — so the producer is where a row too wide for
    /// CDC gets DETECTED (the retirement survivor of the snapshot path's
    /// measureWidestRow): measured for free while encoding, warned loudly.
    event_buf_bytes: usize,
    thread: ?std.Thread = null,

    pub const Cut = struct {
        tenant: []const u8,
        table: []const u8,
        vcol: []const u8,
        tcol: []const u8,
        guarded: bool,
        stream: []const u8,
        cutoff_seq: u64,
        build_ms: i64,
        /// §10ev: when the cut was taken. A message published after it ages out no
        /// earlier than `at_ms` + the stream's max_age, so the cut's own age is the
        /// clock of the age trigger. A cut this process only OBSERVED (a skipped
        /// pair's, made by an earlier tick or process) starts the clock late; the
        /// floor trigger covers what the clock misses.
        at_ms: i64 = 0,
        /// §10ev: a forced cut that failed is not asked for again before this.
        hold_until_ms: i64 = 0,
        /// §10ge: how long this pair's last build that carried a full took (0: none
        /// seen by this process) — what a deferred full would cost.
        full_build_ms: i64 = 0,
        /// §10ge: the edge watch's last reading of the seconds left before the stream
        /// prunes past this cut, and when it was read (0: never read).
        margin_s: f64 = std.math.inf(f64),
        margin_at_ms: i64 = 0,
    };
    pub const Edge = struct { first_seq: u64, at_ms: i64 };
    pub const max_workers: u32 = 32;

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
        hot: ?*hot_streams.HotStreams,
        workers: u32,
    ) GenerationProducer {
        return .{
            .hot = hot,
            .workers = @max(1, @min(workers, max_workers)),
            .edge_scan_seconds = @max(5, cadence_seconds / 4),
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
        if (self.async_fulls) self.full_thread = try std.Thread.spawn(.{}, fullLaneMain, .{self});
    }

    pub fn join(self: *GenerationProducer) void {
        if (self.thread) |t| t.join();
        self.thread = null;
        if (self.full_thread) |t| t.join();
        self.full_thread = null;
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
                // §10eq/§10er: between ticks, watch the edges — the bursting streams every
                // second on the publisher's mark, every cut stream on the slow scan — and
                // re-cut a pair whose splice is about to break.
                if (self.hot) |h| {
                    var ha = std.heap.ArenaAllocator.init(self.allocator);
                    defer ha.deinit();
                    const names = h.hotNow(ha.allocator()) catch &.{};
                    if (names.len > 0) self.edgeCheck(names) catch |err| log.debug("🧬 edge check skipped: {}", .{err});
                }
                if (slept > 0 and slept % self.edge_scan_seconds == 0) {
                    self.edgeCheck(null) catch |err| log.debug("🧬 edge check skipped: {}", .{err});
                }
                utils.sleep(1 * std.time.ns_per_s);
            }
        }
        log.info("🛑 Generation producer stopped", .{});
    }

    fn tick(self: *GenerationProducer) !void {
        return self.tickWith(null);
    }

    /// §10eq: the edge watch. Every CDC stream this producer has cut is asked for its
    /// state; against each pair's newest cut, three questions, any one of which re-cuts
    /// the pair NOW — an empty delta if nothing moved (§10ej) — so the splice never
    /// breaks:
    ///
    ///   1. the RATE: `first_seq` against the previous reading is the prune rate; the
    ///      messages between the cut and the oldest survivor, at that rate, are the
    ///      seconds left; under three of the pair's own build times plus one check
    ///      interval is too few;
    ///   2. the FILL: the stream past 80% of its byte or message cap is about to be
    ///      pruned by a valve, and a cut in the oldest quarter of what it holds is
    ///      the part that goes first — whatever the rate of the last five seconds said,
    ///      since a burst can arrive between two readings;
    ///   3. the FLOOR: a cut within a tenth of the stream's span of its oldest message,
    ///      while the stream prunes at all.
    ///
    /// A stream that is not pruning and not filling costs one STREAM.INFO per check.
    fn edgeCheck(self: *GenerationProducer, only_streams: ?[]const []const u8) !void {
        var snap_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer snap_arena.deinit();
        const cuts = blk: {
            self.cuts_lock.lock();
            defer self.cuts_lock.unlock();
            break :blk try snap_arena.allocator().dupe(Cut, self.cuts.values());
        };
        if (cuts.len == 0) return;
        const wanted = struct {
            fn in(list: ?[]const []const u8, name: []const u8) bool {
                const l = list orelse return true;
                for (l) |n| if (std.mem.eql(u8, n, name)) return true;
                return false;
            }
        };
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();
        var conn_nats = nats.Connection.init(self.allocator, self.io, .{
            .user = self.endpoint.user,
            .password = self.endpoint.pass,
            .nkey_seed = self.endpoint.seed,
            .user_creds = self.endpoint.creds,
            .tls = nats_endpoint.tlsOptions(self.endpoint),
        });
        defer conn_nats.deinit();
        const url = try self.endpoint.dialUrl(alloc);
        try conn_nats.connect(url);
        var js = conn_nats.jetstream(.{ .domain = self.endpoint.js_domain });

        const StreamRead = struct { first: u64, last: u64, rate: f64, fill: f64, to_cap_s: f64, max_age_s: f64 };
        var reads: std.StringArrayHashMapUnmanaged(StreamRead) = .empty;
        const now_ms = utils.unixMillis();
        for (cuts) |cut| {
            if (reads.contains(cut.stream) or !wanted.in(only_streams, cut.stream)) continue;
            var info = js.getStreamInfo(cut.stream) catch continue;
            defer info.deinit();
            const st = info.value.state;
            const cfg = info.value.config;
            var rate: f64 = 0;
            if (self.edges.getPtr(cut.stream)) |e| {
                const dt_s: f64 = @as(f64, @floatFromInt(now_ms - e.at_ms)) / 1000.0;
                if (dt_s > 0 and st.first_seq > e.first_seq) rate = @as(f64, @floatFromInt(st.first_seq - e.first_seq)) / dt_s;
                e.* = .{ .first_seq = st.first_seq, .at_ms = now_ms };
            } else {
                try self.edges.put(self.allocator, try self.allocator.dupe(u8, cut.stream), .{ .first_seq = st.first_seq, .at_ms = now_ms });
            }
            var fill: f64 = 0;
            if (cfg.max_bytes > 0) fill = @max(fill, @as(f64, @floatFromInt(st.bytes)) / @as(f64, @floatFromInt(cfg.max_bytes)));
            if (cfg.max_msgs > 0) fill = @max(fill, @as(f64, @floatFromInt(st.messages)) / @as(f64, @floatFromInt(cfg.max_msgs)));
            // §10es: seconds until the byte cap at the publisher's own fill rate — the
            // one figure that does not depend on a threshold. Infinite when nothing
            // fills or there is no cap; zero once the cap is reached and pruning.
            var to_cap_s: f64 = std.math.inf(f64);
            if (self.hot) |h| if (cfg.max_bytes > 0) {
                const fill_bps = h.fillRate(cut.stream);
                if (fill_bps > 0) {
                    const left: i64 = cfg.max_bytes - @as(i64, @intCast(st.bytes));
                    to_cap_s = if (left <= 0) 0 else @as(f64, @floatFromInt(left)) / @as(f64, @floatFromInt(fill_bps));
                }
            };
            try reads.put(alloc, cut.stream, .{ .first = st.first_seq, .last = st.last_seq, .rate = rate, .fill = fill, .to_cap_s = to_cap_s, .max_age_s = @as(f64, @floatFromInt(cfg.max_age)) / 1e9 });
        }

        var urgent: std.ArrayListUnmanaged(Cut) = .empty;
        for (cuts) |cut| {
            const r = reads.get(cut.stream) orelse continue;
            if (cut.hold_until_ms > now_ms) continue;
            const margin_msgs: i64 = @as(i64, @intCast(cut.cutoff_seq)) - @as(i64, @intCast(r.first));
            const span: i64 = @max(@as(i64, @intCast(r.last)) - @as(i64, @intCast(r.first)), 1);
            const margin_s: f64 = if (margin_msgs <= 0) 0 else if (r.rate > 0) @as(f64, @floatFromInt(margin_msgs)) / r.rate else std.math.inf(f64);
            const need_s: f64 = 3.0 * @as(f64, @floatFromInt(@max(cut.build_ms, 50))) / 1000.0 + @as(f64, @floatFromInt(if (only_streams != null) 1 else self.edge_scan_seconds));
            const by_rate = r.rate > 0 and margin_s < need_s;
            // §10ge: keep the reading — a build deciding whether to defer a full asks it.
            {
                self.cuts_lock.lock();
                defer self.cuts_lock.unlock();
                var key_buf: [512]u8 = undefined;
                if (std.fmt.bufPrint(&key_buf, "{s}.{s}", .{ cut.tenant, cut.table })) |key| {
                    if (self.cuts.getPtr(key)) |live| {
                        live.margin_s = if (r.rate > 0) margin_s else std.math.inf(f64);
                        live.margin_at_ms = now_ms;
                    }
                } else |_| {}
            }
            // Half the span, not a quarter: CDC messages are batches of up to 5,000
            // events, so a stream at its byte cap holds a few hundred of them and a
            // quarter is seconds of margin at a burst's prune rate — one early cut in
            // a 15-minute burst still fell off between two one-second checks (§10er).
            // The cap is near in TIME (at the publisher's fill rate) or in share, and the
            // cut sits in the half that goes first.
            const by_fill = (r.to_cap_s < need_s or r.fill >= 0.8) and margin_msgs < @divTrunc(span, 2);
            const by_floor = r.rate > 0 and margin_msgs < @divTrunc(span, 10);
            // §10ev: two more, for the stream that never pruned by size.
            //   4. GONE: the cut already sits below the oldest message — the age valve
            //      took it in one step on a quiet stream (§10eu) — so every returning
            //      client of this pair waits; the repair cut of §10ej, now, not at
            //      the tick;
            //   5. AGE: a message published after the cut ages out no earlier than the
            //      cut's own age reaches max_age; two scans short of that, with
            //      messages after the cut (an empty stream past its cut cannot lose
            //      anything), re-cut — an empty delta on a quiet pair, once per
            //      max_age, only while other tables keep its stream alive.
            const by_gone = cut.cutoff_seq + 1 < r.first;
            const age_s: f64 = if (cut.at_ms > 0) @as(f64, @floatFromInt(now_ms - cut.at_ms)) / 1000.0 else 0;
            const by_age = r.max_age_s > 0 and cut.at_ms > 0 and r.last > cut.cutoff_seq and
                age_s > r.max_age_s - 2.0 * @as(f64, @floatFromInt(self.edge_scan_seconds));
            if (by_rate or by_fill or by_floor or by_gone or by_age) {
                const why: []const u8 = if (by_gone)
                    "the cut already sits below the stream's oldest message"
                else if (by_age)
                    "the cut is two scans short of the stream's max_age and messages follow it"
                else if (by_rate and by_fill)
                    "the rate leaves under the build time; the fill is past 80% and the cut sits in the oldest half"
                else if (by_rate)
                    "the rate leaves under the build time"
                else if (by_fill)
                    "the fill is past 80% and the cut sits in the oldest half"
                else
                    "the cut is within a tenth of the span";
                log.warn("🧬 '{s}'/'{s}': cutting early — {s} holds {d} message(s), {d:.0}% of a cap, {d:.1} s to the byte cap, pruning {d:.0} msg/s; this pair's cut is {d} message(s) from the oldest and {d:.0} s old ({s})", .{
                    cut.tenant, cut.table, cut.stream, span, r.fill * 100, r.to_cap_s, r.rate, @max(margin_msgs, 0), age_s, why,
                });
                try urgent.append(alloc, cut);
            }
        }
        if (urgent.items.len > 0) try self.tickWith(urgent.items);
    }

    /// The tick, or — with `only` — a rebuild of those pairs alone, forced to cut even
    /// when nothing moved (the edge watch's early cut).
    fn tickWith(self: *GenerationProducer, only: ?[]const Cut) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        // Fresh connections per tick: at generation cadence the handshake cost is
        // noise, and a tick can never inherit a half-dead connection from the last one.
        const cs = try self.openConns(alloc);
        defer cs.close();
        const pgc = cs.pgc;
        const bkc = cs.bkc;
        var js = cs.conn_nats.jetstream(.{ .domain = self.endpoint.js_domain });

        // ── derive the pair list: the publication IS the list ────────────────
        // Minus internals (zebridge_is_internal_table — one predicate, every door),
        // minus keyless tables (the client's guarded upsert needs a key), minus
        // explicit opt-outs (zebridge_enable(generations => false)). Routability and
        // tenancy are decided per table below; GENERATION_RULES, when set, only
        // INTERSECTS what was derived.
        const pub_z = try alloc.dupeZ(u8, self.publication_name);
        if (only) |pairs| {
            const jobs = try alloc.alloc(Job, pairs.len);
            for (pairs, 0..) |cut, i| jobs[i] = .{ .table = cut.table, .tenant = cut.tenant, .vcol = cut.vcol, .tcol = cut.tcol, .guarded = cut.guarded, .force_cut = true };
            self.runJobs(cs, jobs, true);
            return;
        }
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
        var jobs: std.ArrayListUnmanaged(Job) = .empty;
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
                    try jobs.append(alloc, .{ .table = table, .tenant = tenant, .vcol = try alloc.dupe(u8, vcol), .tcol = tcol, .guarded = guarded, .force_cut = false });
                }
            } else {
                const tenant = self.topo.open_tenant;
                if (allowed) |a| {
                    var ok = false;
                    for (a) |cand| ok = ok or std.mem.eql(u8, cand, tenant);
                    if (!ok) continue;
                }
                pairs += 1;
                try jobs.append(alloc, .{ .table = table, .tenant = tenant, .vcol = try alloc.dupe(u8, vcol), .tcol = tcol, .guarded = guarded, .force_cut = false });
            }
        }
        self.runJobs(cs, jobs.items, false);
        log.debug("🧬 tick: {d} published table(s) derived, {d} (table, tenant) pair(s) built or checked", .{ n_tables, pairs });
    }

    /// One (table, tenant) build for the pool: what `buildOne` needs, every string
    /// living in the tick's arena.
    const Job = struct { table: []const u8, tenant: []const u8, vcol: []const u8, tcol: []const u8, guarded: bool, force_cut: bool };

    /// §10ev: the connections one builder holds — PostgreSQL for content and for
    /// bookkeeping, both rendering UTC, and NATS. On the heap: JetStream keeps a
    /// pointer to the connection.
    const Conns = struct {
        pgc: *c.PGconn,
        bkc: *c.PGconn,
        conn_nats: nats.Connection,

        fn close(self: *Conns) void {
            self.conn_nats.deinit();
            if (self.bkc != self.pgc) c.PQfinish(self.bkc);
            c.PQfinish(self.pgc);
        }
    };

    fn openConns(self: *GenerationProducer, alloc: std.mem.Allocator) !*Conns {
        const cs = try alloc.create(Conns);
        const conninfo = try self.pg_config.connInfo(alloc, false);
        const pgc = c.PQconnectdb(conninfo.ptr) orelse return error.ConnectionFailed;
        errdefer c.PQfinish(pgc);
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
        errdefer if (bkc != pgc) c.PQfinish(bkc);
        {
            const res = try queryOne(pgc, "SET timezone TO 'UTC'", &.{});
            c.PQclear(res);
        }

        cs.* = .{ .pgc = pgc, .bkc = bkc, .conn_nats = nats.Connection.init(self.allocator, self.io, .{
            .user = self.endpoint.user,
            .password = self.endpoint.pass,
            .nkey_seed = self.endpoint.seed,
            .user_creds = self.endpoint.creds,
            .tls = nats_endpoint.tlsOptions(self.endpoint),
        }) };
        errdefer cs.conn_nats.deinit();
        const url = try self.endpoint.dialUrl(alloc);
        try cs.conn_nats.connect(url);
        return cs;
    }

    /// §10ev: the builds of a tick, or of the edge watch's early cuts. The cadence
    /// tick builds in turn on its own thread and connections — a quiet fleet of many
    /// tables costs one build's memory at a time. The early cuts, which fire for the
    /// streams that burst (§10ew: `parallel`), go over GENERATION_WORKERS threads —
    /// each with its own connections and arena — taking the next job off a shared
    /// counter, so the round lasts as long as its longest build, not the sum; the
    /// caller's thread drains whatever a worker that could not start or connect left
    /// on the counter, then waits for the rest. A forced cut whose build FAILED holds
    /// its pair for a minute — a dropped table's pair was asked for every second
    /// (§10eu).
    fn runJobs(self: *GenerationProducer, cs: *Conns, jobs: []const Job, parallel: bool) void {
        if (jobs.len == 0) return;
        const n_workers: usize = if (parallel) @min(@as(usize, self.workers), jobs.len) else 1;
        var next = std.atomic.Value(usize).init(0);
        var threads: [max_workers]?std.Thread = .{null} ** max_workers;
        if (n_workers > 1) {
            log.debug("🧬 {d} job(s) over {d} worker(s)", .{ jobs.len, n_workers });
            for (0..n_workers) |w| {
                threads[w] = std.Thread.spawn(.{}, workerMain, .{ self, jobs, &next, w }) catch |err| blk: {
                    log.warn("🧬 worker {d} could not start: {} — its share builds on the tick's thread", .{ w, err });
                    break :blk null;
                };
            }
        }
        self.workLoop(cs, jobs, &next);
        for (threads[0..n_workers]) |t| if (t) |th| th.join();
    }

    fn workerMain(self: *GenerationProducer, jobs: []const Job, next: *std.atomic.Value(usize), w: usize) void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const cs = self.openConns(arena.allocator()) catch |err| {
            log.err("🧬 worker {d}: could not connect: {} — its share builds on the tick's thread", .{ w, err });
            return;
        };
        defer cs.close();
        self.workLoop(cs, jobs, next);
    }

    /// Jobs off the counter until it runs out. Each build gets its own arena, freed
    /// when it is done: a tick of several big tables holds the memory of the builds
    /// IN FLIGHT, not of all its builds (the tick's arena used to hold every one of
    /// them until the tick ended).
    fn workLoop(self: *GenerationProducer, cs: *Conns, jobs: []const Job, next: *std.atomic.Value(usize)) void {
        var js = cs.conn_nats.jetstream(.{ .domain = self.endpoint.js_domain });
        while (true) {
            const i = next.fetchAdd(1, .acq_rel);
            if (i >= jobs.len or self.should_stop.load(.acquire)) return;
            const j = jobs[i];
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const built = self.buildWithRetry(arena.allocator(), cs.pgc, cs.bkc, &js, j.table, j.tenant, j.vcol, j.tcol, j.guarded, j.force_cut);
            if (!built and j.force_cut) self.holdCut(j.tenant, j.table, 60_000);
        }
    }

    fn holdCut(self: *GenerationProducer, tenant: []const u8, table: []const u8, for_ms: i64) void {
        var key_buf: [512]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}.{s}", .{ tenant, table }) catch return;
        self.cuts_lock.lock();
        defer self.cuts_lock.unlock();
        if (self.cuts.getPtr(key)) |cut| cut.hold_until_ms = utils.unixMillis() + for_ms;
    }

    fn queryOnePub(pgc: *c.PGconn, sql: [:0]const u8, params: []const ?[*:0]const u8) !*c.PGresult {
        return queryOne(pgc, sql, params);
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
        /// The payload's own allocator (§10ew): the bytes outlive nothing but the
        /// build, and a growing buffer in an arena leaves every earlier size behind.
        /// The caller frees the returned slice with it.
        payload_alloc: std.mem.Allocator,
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
        var cr = try CopyReader.init(alloc, pgc, select_sql);
        defer cr.deinit();

        // §10ew: the document is WRITTEN as the rows arrive — no value tree. The
        // first version built every row as encoder values and encoded the tree at the
        // end: about 1.1 KB per row held until the build ended, 5.3 GB for four
        // 1.2M-row fulls in flight (§10ev). Now a row costs its msgpack bytes and
        // nothing else. The `rows` array header is array32 with a count patched in at
        // the end, since the count is known only when COPY says so.
        var out: std.ArrayListUnmanaged(u8) = .empty;
        errdefer out.deinit(payload_alloc);
        const rows_hdr = try docHead(&out, payload_alloc, cr.names, prev_cutoff != null, 0);
        while (true) {
            const more = cr.next(&out, payload_alloc) catch |err| {
                try cr.finish(err);
                unreachable;
            };
            if (!more) break;
        }
        try cr.finish(null);

        out_rows.* = cr.nrows;
        if (cr.widest > out_widest.*) out_widest.* = cr.widest;
        std.mem.writeInt(u32, out.items[rows_hdr + 1 ..][0..4], @intCast(cr.nrows), .big);
        try docTail(&out, payload_alloc, gen, kind, cutoff, vcol, prev_cutoff);
        return try out.toOwnedSlice(payload_alloc);
    }

    /// What a full's upload left behind: the counts for the bookkeeping and the logs.
    const FullObject = struct {
        rows: usize,
        widest: usize,
        raw_bytes: u64,
        z_bytes: u64,
        streamed: bool,
    };

    /// §10gi: a full, from the transaction's snapshot to the object `name`, as a stream
    /// (`FullStream`). The row count leads the document, so it is read first in the same
    /// snapshot: one more scan, the price of never holding the table. A COPY the CDC
    /// decoder refuses falls back to the text path, which holds the document (as every
    /// full did before); a NATS failure is returned.
    fn putChainObject(
        alloc: std.mem.Allocator,
        payload_alloc: std.mem.Allocator,
        pgc: *c.PGconn,
        store: *nats.ObjectStore,
        name: []const u8,
        select_sql: [:0]const u8,
        gen: i64,
        cutoff: []const u8,
        vcol: []const u8,
        /// §10gt: "full" (live rows), "checkpoint" (rows moved in a window, tombstones
        /// included) or "delta" — the document says which, and a client applies them
        /// differently.
        kind: []const u8,
        /// A delta's window opener; null for a full or a checkpoint.
        prev_cutoff: ?[]const u8,
    ) !FullObject {
        const expect: usize = blk: {
            const sql = try utils.allocPrintZ(alloc, "SELECT count(*) FROM ({s}) AS q", .{select_sql});
            const res = try queryOne(pgc, sql, &.{});
            defer c.PQclear(res);
            break :blk try std.fmt.parseInt(usize, std.mem.span(c.PQgetvalue(res, 0, 0)), 10);
        };
        streamed: {
            var cr = CopyReader.init(alloc, pgc, select_sql) catch break :streamed;
            defer cr.deinit();
            // Plain zstd, level 3, for every kind of object — a full, a delta and a
            // checkpoint alike. There was a per-era dictionary here until 2026-09-24;
            // measured on the 668 MB test_types full, it gained 53% on 4 KB inputs and 3%
            // from 64 KB up, for a training pass, three columns, an object per era and a
            // client capability a phone did not have (NOTES §10iy).
            const cctx = c.ZSTD_createCCtx() orelse return error.ZstdCompressFailed;
            defer _ = c.ZSTD_freeCCtx(cctx);
            if (c.ZSTD_isError(c.ZSTD_CCtx_setParameter(cctx, c.ZSTD_c_compressionLevel, 3)) != 0) return error.ZstdCompressFailed;
            var fs: FullStream = .{ .cr = &cr, .oa = payload_alloc, .cctx = cctx, .expect_rows = expect, .gen = gen, .kind = kind, .cutoff = cutoff, .prev_cutoff = prev_cutoff, .vcol = vcol };
            defer fs.raw.deinit(payload_alloc);
            _ = try docHead(&fs.raw, payload_alloc, cr.names, prev_cutoff != null, @intCast(expect));
            fs.seen += fs.raw.items.len;
            var info = store.put(.{ .name = name, .opts = .{ .max_chunk_size = store.chunk_size } }, &fs) catch |err| {
                // `put` removed the chunks it had published; the COPY is drained either way.
                cr.finish(err) catch {};
                if (fs.failed == null) return err;
                break :streamed;
            };
            defer info.deinit();
            cr.finish(null) catch |err| {
                store.delete(name) catch {};
                return err;
            };
            return .{ .rows = cr.nrows, .widest = cr.widest, .raw_bytes = fs.seen, .z_bytes = info.value.size, .streamed = true };
        }
        const res = try queryOne(pgc, select_sql, &.{});
        defer c.PQclear(res);
        var rows: usize = 0;
        var widest: usize = 0;
        const payload = try encodeContent(alloc, payload_alloc, res, gen, kind, cutoff, prev_cutoff, vcol, &rows, &widest);
        defer payload_alloc.free(payload);
        const z = try compressZstd(alloc, payload, 3);
        var r = try store.putBytes(name, z);
        r.deinit();
        return .{ .rows = rows, .widest = widest, .raw_bytes = payload.len, .z_bytes = z.len, .streamed = false };
    }

    /// msgpack `{columns, rows, gen, kind, cutoff, prev_cutoff?}` from a text-mode result.
    fn encodeContent(
        alloc: std.mem.Allocator,
        payload_alloc: std.mem.Allocator,
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
        const bytes = try enc.encode(root);
        return try payload_alloc.dupe(u8, bytes);
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
                // "dict": eras cut before 2026-09-24 left a dictionary object per full;
                // named here so a sweep of an old bucket takes them too.
                for ([_][]const u8{ "delta", "full", "ckpt", "dict" }) |kind| {
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
        /// §10eq: the edge watch asks for a cut whatever the table did — a fresh cut
        /// point is the whole point, an empty delta is fine.
        force_cut: bool,
        /// §10ej: set when the cut just published had already fallen off the stream
        /// by the time the manifest was live — read by the caller's bounded retry.
        fell_off: *bool,
    ) !void {
        const build_started_ms = utils.unixMillis();
        const table_z = try alloc.dupeZ(u8, table);
        const tenant_z = try alloc.dupeZ(u8, tenant);
        // §10ff: a table with a publication column list has columns CDC never sends; the
        // chain must not carry them either, or a replica holds values no event updates.
        // The select list is the publication's, `*` when it has none.
        const cols_sel: []const u8 = blk: {
            const pub_z = try alloc.dupeZ(u8, self.publication_name);
            const params = [_]?[*:0]const u8{ pub_z.ptr, table_z.ptr };
            const res = try queryOne(pgc, "SELECT COALESCE((SELECT string_agg(quote_ident(n), ', ' ORDER BY ord) FROM unnest(pt.attnames) WITH ORDINALITY AS u(n, ord)), '*') " ++
                "FROM pg_publication_tables pt WHERE pt.pubname = $1 AND pt.tablename = $2 AND pt.schemaname = 'public'", &params);
            defer c.PQclear(res);
            break :blk if (c.PQntuples(res) > 0) try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0))) else "*";
        };
        // §10ja: every chain object is written in primary-key order. A replica's key index
        // then takes each window as an append instead of a scatter — measured in Node on
        // 3M rows, one window at a time: 115 s arriving in heap order on random keys, 21 s
        // arriving sorted — and a client need not stage and re-sort what PostgreSQL can
        // hand over in order, once per object instead of once per client.
        const order_by = try pkOrderBy(alloc, pgc, table_z);

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
        // §10gl: the next delta's floor (null on rows from before the column: the old
        // tolerance applies once) and the table's relfilenode at the last cut.
        var last_floor: ?[]const u8 = null;
        var last_filenode: ?[]const u8 = null;
        {
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
            const res = try queryOne(bkc, "SELECT gen, cutoff_version::text, row_count, del_count, seed_epoch, col_shape, relid::text, open_xact_floor::text, filenode::text FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND retired_at IS NULL ORDER BY gen DESC LIMIT 1", &params);
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
                if (c.PQgetisnull(res, 0, 7) == 0) last_floor = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 7)));
                if (c.PQgetisnull(res, 0, 8) == 0) last_filenode = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 8)));
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
            last_floor = null;
            last_filenode = null;
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
            const res = try queryOne(bkc, "SELECT gen FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND has_full AND retired_at IS NULL ORDER BY gen DESC LIMIT 1", &params);
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
        // §10gl: what the next delta reads, and what "a version moved" means here.
        const edge_writable = if (self.writable) |w| (w.get(table) orelse true) else true;
        const lower_bound: ?[]const u8 = if (last_cutoff) |cut| try deltaLowerBound(alloc, cut, last_floor, edge_writable) else null;
        if (lower_bound) |lb| {
            const check = try utils.allocPrintZ(alloc, "SELECT EXISTS(SELECT 1 FROM \"{s}\" WHERE \"{s}\" >= {s})", .{ table, vcol, lb });
            const res = try queryOne(pgc, check, &.{});
            defer c.PQclear(res);
            unchanged_by_version = std.mem.eql(u8, std.mem.span(c.PQgetvalue(res, 0, 0)), "f");
        }

        // The next number counts EVERY row, retired ones included: a retired row's
        // objects stay in the bucket for the grace (§10gq), and a chain restarted above
        // an all-retired table (the dictionary's removal retired every era, 2026-09-24)
        // must not name — and overwrite — an object a client may still be reading.
        const gen: i64 = blk: {
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
            const res = try queryOne(bkc, "SELECT COALESCE(max(gen), 0) FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2", &params);
            defer c.PQclear(res);
            break :blk (std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch 0) + 1;
        };
        // A full at gen 1 (nothing to delta against, and the chain needs its jump-in
        // point), then refreshed BEFORE the last one can age out of the kept window
        // (gen > N − depth): rebuild at distance depth − 1 keeps it always inside.
        const build_delta = last_gen > 0;
        var build_full = last_full_gen == 0;
        // The depth rotation: the only full no correctness asks for — it bounds the chain
        // a returning client applies. §10ge decides below whether it waits.
        // §10gw: the chain's shape in one read — what the base weighs, what the
        // checkpoints above it weigh, and whether there are any at all. Both rules below
        // need it, and a second query would let them disagree.
        var base_bytes: i64 = 0;
        var ckpt_bytes: i64 = 0;
        var ckpt_above: i64 = 0;
        if (self.checkpoint_s > 0 and last_gen > 0) {
            const shape_params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };
            const res = queryOne(bkc, "WITH base AS (SELECT COALESCE(max(gen), 0) AS gen FROM public.zebridge_generations " ++
                "                      WHERE tenant=$1 AND tbl=$2 AND has_full AND retired_at IS NULL) " ++
                "SELECT COALESCE((SELECT obj_bytes FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND gen = (SELECT gen FROM base)), 0), " ++
                "       COALESCE(sum(obj_bytes), 0), count(*) " ++
                "FROM public.zebridge_generations " ++
                "WHERE tenant=$1 AND tbl=$2 AND has_checkpoint AND retired_at IS NULL AND gen > (SELECT gen FROM base)", &shape_params) catch null;
            if (res) |r| {
                defer c.PQclear(r);
                base_bytes = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(r, 0, 0)), 10) catch 0;
                ckpt_bytes = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(r, 0, 1)), 10) catch 0;
                ckpt_above = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(r, 0, 2)), 10) catch 0;
            }
        }

        // §10gw: with checkpoints on, the generation COUNT no longer asks for a full — the
        // base is rebuilt by a correctness rule, by the size rule, or when the checkpoints
        // outweigh it (above), never on a clock. The depth rule stays as a VALVE at four
        // times the depth: a lane that has stalled leaves the chain growing with no
        // checkpoint to prune against, and this bounds that without undoing the design.
        const depth_reach: i64 = if (self.checkpoint_s > 0)
            @as(i64, self.chain_depth) * config.Generations.full_defer_factor
        else
            @as(i64, self.chain_depth) - 1;
        // ⚠️ And the valve only opens when the lane has actually STALLED — no checkpoint
        // above the base at all. Measured without this: at 100k events a second the valve
        // fired every 24 generations (~50 s) and rebuilt six whole-table fulls in five
        // minutes, while the lane was cutting checkpoints perfectly well. That is the very
        // thing checkpoints exist to stop.
        const lane_stalled = self.checkpoint_s == 0 or ckpt_above == 0;
        const depth_due = last_full_gen != 0 and (gen - last_full_gen) >= depth_reach and lane_stalled;
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
        // …and the stream's `created`: a client gates on `cutoff_seq` only while it reads
        // the SAME incarnation of the stream. A stream deleted and recreated (a wipe, a
        // lost slot) restarts its numbering, and a manifest cut before that carries a
        // seq that means nothing on it — the numbers alone cannot say so (measured:
        // stream_wipe.py, every replayed event dropped as "in the chain").
        const cutoff_seq: u64, const cdc_stream: []const u8, const stream_first: u64, const stream_created: []const u8 = blk: {
            const name = if (std.mem.eql(u8, tenant, self.topo.open_tenant))
                self.topo.cdc_stream_public
            else
                try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.topo.cdc_stream_prefix, tenant });
            if (js.getStreamInfo(name)) |info_const| {
                var info = info_const;
                defer info.deinit();
                break :blk .{ info.value.state.last_seq, try alloc.dupe(u8, name), info.value.state.first_seq, try alloc.dupe(u8, info.value.created) };
            } else |err| {
                log.warn("🧬 '{s}'/'{s}': stream info for {s} failed ({}) — manifest ships without cutoff_seq, clients use the legacy lsn gate", .{ tenant, table, name, err });
                break :blk .{ 0, name, 0, "" };
            }
        };
        // §10ei: the chain must OVERLAP the stream. A client that fell off the stream
        // seeds from the newest manifest and resumes at its cutoff_seq; when the stream
        // no longer holds that sequence (a size valve, a purge, an age under the floor
        // of §10eg) the client says "predates the stream" and waits for the NEXT
        // generation — which an unchanged table would never get. So the previous
        // cutoff is checked against the stream's oldest message here, and a chain that
        // fell off forces a build even when nothing moved.
        // The previous manifest's cut: the edge watch's memory of a pair this tick does
        // not rebuild (§10eq), and the fall-off test (§10ei).
        const prev_cut: i64 = blk: {
            if (last_gen == 0) break :blk 0;
            var kvb = js.kvBucket(self.topo.kv_generations) catch break :blk 0;
            defer kvb.deinit();
            const mkey = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ tenant, table });
            var entry = kvb.get(mkey) catch break :blk 0;
            defer entry.deinit();
            const man = std.json.parseFromSliceLeaky(std.json.Value, alloc, entry.value, .{}) catch break :blk 0;
            if (man != .object) break :blk 0;
            break :blk if (man.object.get("cutoff_seq")) |v| (if (v == .integer) v.integer else 0) else 0;
        };
        const chain_fell_off: bool = prev_cut > 0 and stream_first > 1 and prev_cut + 1 < @as(i64, @intCast(stream_first));
        if (chain_fell_off) log.warn("🧬 '{s}'/'{s}': chain g{d} fell off {s} (its cutoff is below the stream's oldest message, seq {d}) — a returning client could not splice; cutting a delta with a fresh cut point", .{ tenant, table, last_gen, cdc_stream, stream_first });

        // ── §10gl: the next delta's floor, read JUST BEFORE the snapshot ────
        // A row this snapshot cannot see and a later commit makes visible comes from a
        // transaction that was open when the snapshot was taken, or began after it; its
        // `now()` is its start, so its version is at least the start of the oldest
        // transaction open at this instant, or this instant. Read inside the snapshot
        // instead, a transaction that committed between the two would already be gone
        // from pg_stat_activity. Null when the database predates the function: the next
        // delta keeps the old fixed tolerance.
        const floor_now: ?[]const u8 = blk: {
            const probe = try queryOne(pgc, "SELECT to_regprocedure('public.zebridge_oldest_open_xact()') IS NOT NULL", &.{});
            const present = c.PQgetvalue(probe, 0, 0)[0] == 't';
            c.PQclear(probe);
            if (!present) {
                if (!self.floor_warned.swap(true, .acquire)) log.warn("🧬 public.zebridge_oldest_open_xact() is missing — deltas keep the fixed {s} overlap, and a transaction open longer than that across a cut can be missed; apply init.core.template.sql", .{config.Sync.version_future_tolerance});
                break :blk null;
            }
            const res = try queryOne(pgc, "SELECT LEAST(now(), COALESCE(public.zebridge_oldest_open_xact(), now()))::text", &.{});
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

        // §10gt: the sweeper's watermark as it stands, carried in the manifest. A
        // returning client compares its own against it FIRST (§10gs): below it, a row it
        // holds may have been deleted AND reaped while it was away, and no delta or
        // checkpoint carries an absence — only the base does. Its own copy of
        // zebridge_gc_watermark is as old as its last sync, so the answer has to travel
        // with the chain. Empty when the sweeper has never run: the planner then treats
        // the chain as it did before, which is what a deployment without a sweeper wants.
        const gc_watermark: []const u8 = blk: {
            const res = queryOne(pgc, "SELECT watermark::text FROM public.zebridge_gc_watermark LIMIT 1", &.{}) catch break :blk "";
            defer c.PQclear(res);
            if (c.PQntuples(res) == 0) break :blk "";
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
        //
        // §10gl: only on a table WITHOUT the delete guard. On a guarded table every DELETE
        // is a tombstone (a version move) and the counter below only moves for the
        // sweeper's reaps, so the count told nothing but a TRUNCATE — which the table's
        // relfilenode tells exactly, for free. The count was a full scan per cut: 236 ms
        // at 17M rows, the largest part of a 100k-events/s delta after its rows. It stays
        // where hard deletes are real: n_tup_del is no substitute there, since a backend
        // publishes it only between transactions (measured: a DELETE followed by a 20 s
        // statement in the same session was still invisible after 19 s).
        const row_count_now: ?i64 = if (guarded) null else blk: {
            const sql = try utils.allocPrintZ(alloc, "SELECT count(*) FROM \"{s}\"", .{table});
            const res = try queryOne(pgc, sql, &.{});
            defer c.PQclear(res);
            break :blk std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch 0;
        };
        // The table's storage at the cut, from pg_class under this snapshot.
        const filenode_now: []const u8 = blk: {
            if (cur_relid.len == 0) break :blk "";
            const res = try queryOne(pgc, "SELECT relfilenode::text FROM pg_class WHERE oid = $1::oid", &.{(try alloc.dupeZ(u8, cur_relid)).ptr});
            defer c.PQclear(res);
            if (c.PQntuples(res) == 0) break :blk "";
            break :blk try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0)));
        };
        const filenode_moved = guarded and last_filenode != null and filenode_now.len > 0 and !std.mem.eql(u8, last_filenode.?, filenode_now);
        // ⚠️ The count's blind spot, closed: N inserts exactly offset by N deletes of
        // rows untouched since the cutoff leave count(*) unchanged. The statistics
        // collector's n_tup_del is cumulative per table and only ever grows — if it
        // moved, something was deleted, whatever the count says. It lags a commit by
        // up to ~500 ms, far inside a cadence; a stats reset drops it to 0, which reads
        // as "moved" and costs one extra full. NULL for a table the collector has not
        // seen yet: treated as 0.
        // `table_rows`: the size the §10es rule compares a delta with — the count where
        // one was taken, the collector's estimate on a guarded table (a heuristic either way).
        var table_rows: i64 = row_count_now orelse 0;
        const del_count_now: i64 = blk: {
            const res = try queryOne(pgc, "SELECT COALESCE(n_tup_del, 0), COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE schemaname = 'public' AND relname = $1", &.{table_z.ptr});
            defer c.PQclear(res);
            if (c.PQntuples(res) == 0) break :blk 0;
            if (row_count_now == null) table_rows = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 1)), 10) catch 0;
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
        // What the last cut recorded to compare with: its delete count, and its row
        // count (unguarded) or its relfilenode (guarded). Missing on rows from before
        // those columns: build once to record them.
        const recorded = last_del_count != null and (if (guarded) last_filenode != null else last_row_count != null);
        // No row went without a version move since the last cut.
        const rows_held = if (guarded) !filenode_moved else (recorded and (last_row_count.? == row_count_now.? or shrink_explained));
        if (unchanged_by_version) {
            if (!recorded) {
                log.info("🧬 '{s}'/'{s}': nothing recorded to compare with at g{d} ({s}) — building once to record it", .{ tenant, table, last_gen, if (guarded) "relfilenode or delete count" else "row count or delete count" });
            } else if (rows_held and !deletes_moved and !epoch_moved and !shape_moved) {
                // An epoch move is a change even when nothing else moved (§10df): the
                // whole point of zebridge_reseed() is a full for data CDC never carried.
                if (!chain_fell_off and !force_cut) {
                    log.debug("🧬 '{s}'/'{s}': unchanged since g{d} ({d} deletes) — skipped", .{ tenant, table, last_gen, del_count_now });
                    const rb = try queryOne(pgc, "ROLLBACK", &.{});
                    c.PQclear(rb);
                    // §10eq: a skipped pair still has a cut to watch — the previous
                    // one, with a conservative build time until this process builds it.
                    if (prev_cut > 0) self.recordCut(tenant, table, vcol, tcol, guarded, cdc_stream, @intCast(prev_cut), 100, false) catch {};
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
            } else if (filenode_moved) {
                log.info("🧬 '{s}'/'{s}': no version moved since g{d} but the table's storage was replaced (relfilenode {s} -> {s}: TRUNCATE, or a rewrite) — forcing a full", .{ tenant, table, last_gen, last_filenode.?, filenode_now });
            } else if (!guarded and last_row_count.? != row_count_now.?) {
                log.info("🧬 '{s}'/'{s}': no version moved since g{d} but the row count did ({d} -> {d}) — forcing a full", .{ tenant, table, last_gen, last_row_count.?, row_count_now.? });
            } else {
                log.info("🧬 '{s}'/'{s}': no version moved and the count held since g{d}, but n_tup_del moved ({d} -> {d}) — deletes offset by inserts; forcing a full", .{ tenant, table, last_gen, last_del_count.?, del_count_now });
            }
            if (!repair_only) build_full = true;
        } else if (deletes_moved) {
            // Changed by version AND something was deleted: the delta carries what
            // moved, a full is the only thing that can carry an absence.
            log.info("🧬 '{s}'/'{s}': n_tup_del moved since g{d} ({d} -> {d}) — forcing a full alongside the delta", .{ tenant, table, last_gen, last_del_count.?, del_count_now });
            build_full = true;
        } else if (filenode_moved) {
            log.info("🧬 '{s}'/'{s}': the table's storage was replaced since g{d} (relfilenode {s} -> {s}: TRUNCATE, or a rewrite) — forcing a full alongside the delta", .{ tenant, table, last_gen, last_filenode.?, filenode_now });
            build_full = true;
        } else if (!guarded and last_row_count != null and row_count_now.? < last_row_count.? and !shrink_explained) {
            // Changed by version AND shrunk: the delta will carry the survivors that
            // moved, but nothing can carry the rows that went — a full must.
            log.info("🧬 '{s}'/'{s}': row count shrank since g{d} ({d} -> {d}) — forcing a full alongside the delta", .{ tenant, table, last_gen, last_row_count.?, row_count_now.? });
            build_full = true;
        }

        // ── §10ge: the depth rotation's full, now or later ───────────────────
        // Every full above this line is owed (first, epoch, shape, deletes, repair).
        // The rotation's is not: while the stream holds fewer seconds past this pair's
        // cut than three of its full builds, a full here is the build most likely to
        // be pruned past (§10gd: fulls set the edge, deltas are cheap), so the chain
        // continues on deltas — up to depth × full_defer_factor generations.
        if (depth_due and !build_full) {
            const since = gen - last_full_gen;
            const limit: i64 = @as(i64, self.chain_depth) * config.Generations.full_defer_factor - 1;
            const reading = self.deferReading(tenant, table);
            const decision = fullDecision(self.defer_fulls, since, limit, reading.margin_s, utils.unixMillis() - reading.margin_at_ms, reading.margin_at_ms != 0, reading.full_build_ms, self.edge_scan_seconds);
            switch (decision) {
                .build, .build_at_limit => {
                    if (decision == .build_at_limit)
                        log.warn("🧬 '{s}'/'{s}': the full was deferred {d} generation(s), the limit — building it now, {d:.1} s of margin against a {d} ms full", .{ tenant, table, since, reading.margin_s, reading.full_build_ms });
                    // §10gf: in the background, behind the deltas, unless the lane has
                    // fallen twice the limit behind (a full it keeps discarding) — then here.
                    if (self.async_fulls and since < 2 * limit) {
                        if (self.requestFull(tenant, table, vcol, tcol, .full))
                            log.info("🧬 '{s}'/'{s}': the depth rotation's full goes to the background lane (g{d} is the last full, {d} generation(s) since); this build cuts the delta", .{ tenant, table, last_full_gen, since });
                    } else build_full = true;
                },
                .defer_full => log.info("🧬 '{s}'/'{s}': deferring the depth rotation's full — {d:.1} s of margin, under 3 × the last full's {d} ms + {d} s; g{d} stays the full ({d} of {d} generations)", .{ tenant, table, reading.margin_s, reading.full_build_ms, self.edge_scan_seconds, last_full_gen, since, limit }),
            }
        }

        // ── §10gt: is a checkpoint due? ──────────────────────────────────────
        // The middle level: every `checkpoint_s` the lane cuts one covering the rows that
        // moved since the last checkpoint (or the last full), tombstones included. It is
        // asked for here, where the pair's state is already in hand, and built behind the
        // deltas — like the depth rotation's full, and for the same reason: it must never
        // hold a cut. A full already queued for this pair wins (it carries everything a
        // checkpoint would), which `requestFull` settles by keeping one job per pair.
        if (self.checkpoint_s > 0 and self.async_fulls and !build_full and last_gen > 0 and
            table_rows < config.Generations.min_checkpoint_rows)
        {
            log.debug("🧬 '{s}'/'{s}': no checkpoint — {d} row(s) is under the {d} the middle level is worth cutting for (a guarded table reads the collector's estimate, which is 0 until ANALYZE)", .{ tenant, table, table_rows, config.Generations.min_checkpoint_rows });
        }
        if (self.checkpoint_s > 0 and self.async_fulls and !build_full and last_gen > 0 and
            table_rows >= config.Generations.min_checkpoint_rows)
        {
            const params_ck = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, (try utils.allocPrintZ(alloc, "{d}", .{self.checkpoint_s})).ptr };
            const due = queryOne(bkc, "SELECT COALESCE(max(cutoff_version) < now() - ($3 || ' seconds')::interval, false) " ++
                "FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND (has_checkpoint OR has_full) AND retired_at IS NULL", &params_ck) catch null;
            if (due) |d| {
                defer c.PQclear(d);
                if (c.PQntuples(d) > 0 and c.PQgetvalue(d, 0, 0)[0] == 't') {
                    if (self.requestFull(tenant, table, vcol, tcol, .checkpoint))
                        log.info("🧬 '{s}'/'{s}': a checkpoint is due ({d} s since the last one) — the background lane cuts it behind the deltas", .{ tenant, table, self.checkpoint_s });
                }
            }
        }

        // ── §10gw: has the base been outweighed? ─────────────────────────────
        // Once the generation count stops deciding, this is the only reason a base is
        // rebuilt without a correctness rule asking: when the checkpoints since it weigh
        // more than it does, a returning client applying them pays more than a reload.
        if (self.checkpoint_s > 0 and self.async_fulls and !build_full and
            base_bytes > 0 and ckpt_bytes * 100 > base_bytes * @as(i64, self.base_rebuild_percent))
        {
            if (self.requestFull(tenant, table, vcol, tcol, .full))
                log.info("🧬 '{s}'/'{s}': the {d} checkpoint(s) since the base weigh {d} bytes against its {d} ({d}% is the bar) — the lane rebuilds the base", .{ tenant, table, ckpt_above, ckpt_bytes, base_bytes, self.base_rebuild_percent });
        }

        // ── 3. content: full and/or delta against the SAME snapshot ──────────
        // §10gi: the store is opened before the content, since a full is written to it
        // while COPY reads (the snapshot stays open for the upload).
        const bucket = try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.topo.generation_bucket_prefix, tenant });
        var osm = js.objectStoreManager();
        var store = osm.openStore(bucket) catch |err| blk: {
            if (err == error.StoreNotFound or err == error.StreamNotFound) {
                break :blk try osm.createStore(.{ .store_name = bucket, .description = "ZeBridge generations (NOTES.md §1.13)" });
            }
            return err;
        };
        defer store.deinit();
        const full_name = try std.fmt.allocPrint(alloc, "{s}-g{d}-full", .{ table, gen });
        var full_obj: ?FullObject = null;
        var full_rows: usize = 0;
        var widest_row: usize = 0;
        // §10eo: where a build's time goes, one line per generation — the number that
        // decides whether the producer's per-row cost is the query, the encode, the
        // compression or the upload, before anyone parallelises the wrong stage.
        var ph: struct { query: i64 = 0, full: i64 = 0, zstd: i64 = 0, upload: i64 = 0 } = .{};
        if (build_full) {
            // §10ek: a full carries LIVE rows only. Every client applies a full as a wipe
            // and a reload inside one transaction, so a row absent from it is gone on the
            // replica whether or not the full named it; a tombstoned row in a full only
            // told the client to delete what it had already deleted. The fast sting's
            // full was 15 MiB for four live rows. The DELTA keeps its tombstoned rows:
            // that is the delete signal for a client catching up.
            const sql = if (tcol.len > 0)
                try utils.allocPrintZ(alloc, "SELECT {s} FROM \"{s}\" WHERE \"{s}\" IS NULL{s}", .{ cols_sel, table, tcol, order_by })
            else
                try utils.allocPrintZ(alloc, "SELECT {s} FROM \"{s}\"{s}", .{ cols_sel, table, order_by });
            const t_f = utils.unixMillis();
            full_obj = try putChainObject(alloc, self.allocator, pgc, &store, full_name, sql, gen, cutoff_version, vcol, "full", null);
            ph.full += utils.unixMillis() - t_f;
            full_rows = full_obj.?.rows;
            widest_row = @max(widest_row, full_obj.?.widest);
        }

        // §10gx: the delta streams too — COPY straight through the compressor into the
        // object store. It was the last artifact
        // built whole in memory: three copies of it overlapped at the peak (the buffer
        // doubling as it grew, the `compressBound` destination, and the tick's arena),
        // which is where the gigabytes at 100k events a second came from (§10gj).
        var delta_obj: ?FullObject = null;
        var delta_rows: usize = 0;
        if (build_delta) {
            const sql = try utils.allocPrintZ(alloc, "SELECT {s} FROM \"{s}\" WHERE \"{s}\" >= {s}{s}", .{ cols_sel, table, vcol, lower_bound.?, order_by });
            const delta_name = try std.fmt.allocPrint(alloc, "{s}-g{d}-delta", .{ table, gen });
            const t_q = utils.unixMillis();
            delta_obj = try putChainObject(alloc, self.allocator, pgc, &store, delta_name, sql, gen, cutoff_version, vcol, "delta", last_cutoff);
            delta_rows = delta_obj.?.rows;
            widest_row = @max(widest_row, delta_obj.?.widest);
            ph.query += utils.unixMillis() - t_q;
        }
        size_rule: {
        // §10es: the size rule beside the depth rule. A delta carries every row whose
        // version moved since the last cut, so a burst that re-stamps the same rows
        // puts them into every delta cut while it lasts, and a client catching up
        // applies them once per delta — five deltas of 700,000 rows for a table of
        // 75,000 live rows, nine copies of each row, 900 MB raw where a full is 19 MB.
        // A full costs the producer the same 200 ms and carries each row once: when
        // the delta would carry more than half the table, cut the full with it. A
        // client with an old watermark takes the full; one with a recent watermark
        // takes the one delta it needs anyway.
        if (build_delta and !build_full and delta_rows > 0 and delta_rows * 2 > @as(usize, @intCast(@max(table_rows, 1)))) {
            // §10gr: to the background lane, like the depth rotation's (§10gf). This full
            // is an ECONOMY for a client catching up, never a correctness debt — the delta
            // beside it carries every row that moved — so it must not hold the cut. Built
            // here it was the last hole left in every firehose run: 9.7 s at 100k events a
            // second, 14.1 s at 150k, and the cut fell off the stream while it ran. A
            // client that seeds a few seconds later gets the same economy from the lane.
            if (self.async_fulls) {
                // A refused request means one is already queued for this pair, which
                // serves the same purpose: either way this cut publishes its delta alone.
                if (self.requestFull(tenant, table, vcol, tcol, .full)) {
                    log.info("🧬 '{s}'/'{s}': the delta carries {d} of the table's {d} row(s) — a full is cheaper for a client catching up; the background lane builds one, this cut publishes the delta alone", .{ tenant, table, delta_rows, table_rows });
                } else {
                    log.debug("🧬 '{s}'/'{s}': the delta carries {d} of {d} row(s); a full is already queued for the lane", .{ tenant, table, delta_rows, table_rows });
                }
                break :size_rule;
            }
            log.info("🧬 '{s}'/'{s}': the delta carries {d} of the table's {d} row(s) — a full is cheaper for every client catching up; cutting one alongside", .{ tenant, table, delta_rows, table_rows });
            build_full = true;
            const sql = if (tcol.len > 0)
                try utils.allocPrintZ(alloc, "SELECT {s} FROM \"{s}\" WHERE \"{s}\" IS NULL{s}", .{ cols_sel, table, tcol, order_by })
            else
                try utils.allocPrintZ(alloc, "SELECT {s} FROM \"{s}\"{s}", .{ cols_sel, table, order_by });
            const t_f = utils.unixMillis();
            full_obj = try putChainObject(alloc, self.allocator, pgc, &store, full_name, sql, gen, cutoff_version, vcol, "full", null);
            ph.full += utils.unixMillis() - t_f;
            full_rows = full_obj.?.rows;
            widest_row = @max(widest_row, full_obj.?.widest);
        }
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

        // ── 4. immutable objects first (the full and the delta are written, §10gi/§10gx) ──
        // Chain objects ship as zstd frames (§10w): built once, read by every
        // client forever — the one payload where compression amortizes fully.
        // Clients detect by the standard 4-byte magic, so mixed chains (older
        // uncompressed deltas referenced by the same manifest) keep working and
        // no object is ever rewritten. Fulls took level 9 as "read-many"; measured on a
        // 75,000-row full (§10eo) level 9 was 138 ms of a 327 ms build, 1.8 µs a row —
        // the producer's single largest cost, and the producer is the side that races
        // the stream (§10ej). Level 3 here, like the deltas; the ratio is compared below.
        if (full_obj) |fo| {
            log.info("🗜️ '{s}'/'{s}': g{d} full {d} -> {d} bytes ({d}%){s}", .{ tenant, table, gen, fo.raw_bytes, fo.z_bytes, fo.z_bytes * 100 / @max(fo.raw_bytes, 1), if (fo.streamed) " [streamed]" else "" });
        }
        if (delta_obj) |d| {
            log.info("🗜️ '{s}'/'{s}': g{d} delta {d} -> {d} bytes ({d}%){s}", .{ tenant, table, gen, d.raw_bytes, d.z_bytes, d.z_bytes * 100 / @max(d.raw_bytes, 1), if (d.streamed) " [streamed]" else "" });
        }

        // ── 5. the chain manifest, swapped last ──────────────────────────────
        // Built from the kept window's committed rows plus this generation in memory
        // (its row does not exist yet — see the ordering note above).
        var full_gen_m: i64 = 0;
        var full_cutoff_m: []const u8 = "";
        // Retire BEFORE rendering: the manifest below is built from the rows that survive.
        try pruneChain(alloc, bkc, &store, tenant_z, table_z, table, keepFrom(gen, self.chain_depth, if (build_full) gen else last_full_gen), self.retire_grace_s, self.retire_windows, self.checkpoint_s > 0, if (build_full) gen else 0);
        var deltas_json: std.ArrayList(u8) = .empty;
        {
            const keep_from = try utils.allocPrintZ(alloc, "{d}", .{keepFrom(gen, self.chain_depth, if (build_full) gen else last_full_gen)});
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, keep_from.ptr };
            const res = try queryOne(bkc, "SELECT gen, cutoff_version::text, COALESCE(prev_cutoff::text, ''), has_full " ++
                "FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND gen > $3 AND retired_at IS NULL ORDER BY gen", &params);
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
                    const frag = try std.fmt.allocPrint(alloc, "{s}{{\"gen\":{s},\"object\":\"{s}-g{s}-delta\",\"prev_cutoff\":\"{s}\",\"cutoff\":\"{s}\"}}", .{ if (deltas_json.items.len > 0) "," else "", g, table, g, prev, cutoff });
                    try deltas_json.appendSlice(alloc, frag);
                }
            }
        }
        if (build_full) {
            full_gen_m = gen;
            full_cutoff_m = cutoff_version;
        }
        if (build_delta) {
            const frag = try std.fmt.allocPrint(alloc, "{s}{{\"gen\":{d},\"object\":\"{s}-g{d}-delta\",\"prev_cutoff\":\"{s}\",\"cutoff\":\"{s}\"}}", .{ if (deltas_json.items.len > 0) "," else "", gen, table, gen, last_cutoff.?, cutoff_version });
            try deltas_json.appendSlice(alloc, frag);
        }

        // §10gt: the checkpoints above the chain's full, rendered from the same rows the
        // background lane attaches them to.
        const ckpts_json: []const u8 = blk: {
            const arr = checkpointsJson(alloc, bkc, table, tenant_z, table_z, full_gen_m) catch break :blk "[]";
            var out: std.Io.Writer.Allocating = .init(alloc);
            std.json.Stringify.value(std.json.Value{ .array = arr }, .{}, &out.writer) catch break :blk "[]";
            break :blk out.written();
        };
        var kv = js.kvBucket(self.topo.kv_generations) catch blk: {
            var km = js.kvManager();
            break :blk try km.createBucket(.{ .bucket = self.topo.kv_generations, .history = 1 });
        };
        defer kv.deinit();
        const key = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ tenant, table });
        const seq_frag: []const u8 = if (cutoff_seq > 0)
            try std.fmt.allocPrint(alloc, "\"cutoff_seq\":{d},\"cdc_stream\":\"{s}\",\"cdc_stream_created\":\"{s}\",", .{ cutoff_seq, cdc_stream, stream_created })
        else
            "";
        const gc_frag: []const u8 = if (gc_watermark.len > 0)
            try std.fmt.allocPrint(alloc, "\"gc_watermark\":\"{s}\",", .{gc_watermark})
        else
            "";
        const manifest = try std.fmt.allocPrint(alloc, "{{\"gen\":{d},\"seed_epoch\":{d},\"bucket\":\"{s}\",{s}{s}\"cutoff_version\":\"{s}\",\"cutoff_lsn\":\"{s}\"," ++
            "\"version_column\":\"{s}\"," ++
            "\"full\":{{\"gen\":{d},\"object\":\"{s}-g{d}-full\",\"cutoff\":\"{s}\"}},\"checkpoints\":{s},\"deltas\":[{s}]}}", .{ gen, cat_epoch, bucket, seq_frag, gc_frag, cutoff_version, lsn, vcol, full_gen_m, table, full_gen_m, full_cutoff_m, ckpts_json, deltas_json.items });
        _ = try kv.put(key, manifest, .{});

        // ── objects and manifest live: NOW the row becomes the producer's memory ──
        {
            const gen_str = try utils.allocPrintZ(alloc, "{d}", .{gen});
            const lsn_z = try alloc.dupeZ(u8, lsn);
            const cut_z = try alloc.dupeZ(u8, cutoff_version);
            const prev_z: ?[*:0]const u8 = if (last_cutoff) |p| (try alloc.dupeZ(u8, p)).ptr else null;
            const count_z: ?[*:0]const u8 = if (row_count_now) |n| (try utils.allocPrintZ(alloc, "{d}", .{n})).ptr else null;
            const floor_z: ?[*:0]const u8 = if (floor_now) |f| (try alloc.dupeZ(u8, f)).ptr else null;
            const filenode_z: ?[*:0]const u8 = if (filenode_now.len > 0) (try alloc.dupeZ(u8, filenode_now)).ptr else null;
            // §10gw: what this row's full weighs, for the rule that decides the next base.
            const obj_bytes_z: ?[*:0]const u8 = if (full_obj) |fo| (try utils.allocPrintZ(alloc, "{d}", .{fo.z_bytes})).ptr else null;
            const del_z = try utils.allocPrintZ(alloc, "{d}", .{del_count_now});
            const epoch_z = try utils.allocPrintZ(alloc, "{d}", .{cat_epoch});
            const shape_z = try alloc.dupeZ(u8, col_shape);
            const relid_z: ?[*:0]const u8 = if (cur_relid.len > 0) (try alloc.dupeZ(u8, cur_relid)).ptr else null;
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, gen_str.ptr, cut_z.ptr, lsn_z.ptr, prev_z, if (build_full) "t" else "f", count_z, del_z.ptr, epoch_z.ptr, shape_z.ptr, relid_z, floor_z, filenode_z, obj_bytes_z };
            const res = try queryOne(bkc, "INSERT INTO public.zebridge_generations (tenant, tbl, gen, cutoff_version, cutoff_lsn, prev_cutoff, has_full, row_count, del_count, seed_epoch, col_shape, relid, open_xact_floor, filenode, obj_bytes) " ++
                "VALUES ($1, $2, $3, $4::timestamptz, $5::pg_lsn, $6::timestamptz, $7::boolean, $8::bigint, $9::bigint, $10::integer, $11, $12::oid, $13::timestamptz, $14::oid, $15::bigint) " ++
                "ON CONFLICT (tenant, tbl, gen) DO NOTHING", &params);
            c.PQclear(res);
        }

        // §10eq: the edge watch's memory of this pair.
        self.recordCut(tenant, table, vcol, tcol, guarded, cdc_stream, cutoff_seq, utils.unixMillis() - build_started_ms, true) catch |err| log.debug("🧬 cut not recorded: {}", .{err});
        if (build_full) self.recordFullBuild(tenant, table, utils.unixMillis() - build_started_ms);

        // ── 6. (pruning ran before the manifest was rendered — see pruneChain) ──

        // The duration is the number the retention contract needs (§10em): the CDC
        // window must cover two cadences AND this, since the cut is taken before the
        // build and must still be in the stream when the manifest is live.
        log.info("🧬 g{d} for '{s}'/'{s}': {s}{s}{s} → {s} (cutoff {s} @ {s}) in {d} ms", .{
            gen,                                   tenant,                                      table,
            if (build_delta) "delta" else "",      if (build_delta and build_full) "+" else "", if (build_full) "full" else "",
            bucket,                                cutoff_version,                              lsn,
            utils.unixMillis() - build_started_ms,
        });
        // §10gx: both artifacts are one streamed phase now — COPY, encode, zstd and the
        // upload happen together, so there is nothing left to time apart.
        log.info("🧬   phases: full (count+copy+encode+zstd+upload) {d} ms, delta (count+copy+encode+zstd+upload) {d} ms — {d} full row(s), {d} delta row(s)", .{ ph.full, ph.query, full_rows, delta_rows });
        if (delta_obj) |d| log.debug("🧬   delta: {d} row(s), {d} bytes", .{ delta_rows, d.raw_bytes });
        if (full_obj) |fo| log.debug("🧬   full:  {d} row(s), {d} bytes", .{ full_rows, fo.raw_bytes });

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
                    fell_off.* = true;
                    log.warn("🧬 '{s}'/'{s}': g{d}'s cut (seq {d}) fell off {s} during the build — the stream's oldest message is {d} now, the build took {d} ms; no client can splice on this generation", .{ tenant, table, gen, cutoff_seq, cdc_stream, first_now, utils.unixMillis() - build_started_ms });
                }
            } else |_| {}
        }
    }

    /// `just_cut`: this build published the cut (the clock starts now, a hold is
    /// lifted); false for a cut only observed on a skipped pair (the clock keeps
    /// what it had, or starts now if this process never saw the pair).
    fn recordCut(self: *GenerationProducer, tenant: []const u8, table: []const u8, vcol: []const u8, tcol: []const u8, guarded: bool, stream: []const u8, cutoff_seq: u64, build_ms: i64, just_cut: bool) !void {
        if (cutoff_seq == 0 or stream.len == 0) return;
        const key = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ tenant, table });
        self.cuts_lock.lock();
        defer self.cuts_lock.unlock();
        if (self.cuts.getPtr(key)) |cut| {
            self.allocator.free(key);
            if (just_cut or cut.cutoff_seq != cutoff_seq) cut.at_ms = utils.unixMillis();
            cut.cutoff_seq = cutoff_seq;
            cut.build_ms = build_ms;
            if (just_cut) cut.hold_until_ms = 0;
            return;
        }
        try self.cuts.put(self.allocator, key, .{
            .tenant = try self.allocator.dupe(u8, tenant),
            .table = try self.allocator.dupe(u8, table),
            .vcol = try self.allocator.dupe(u8, vcol),
            .tcol = try self.allocator.dupe(u8, tcol),
            .guarded = guarded,
            .stream = try self.allocator.dupe(u8, stream),
            .cutoff_seq = cutoff_seq,
            .build_ms = build_ms,
            .at_ms = utils.unixMillis(),
        });
    }

    /// §10ge: what the deferral decision reads from the pair's cut record.
    const DeferReading = struct { margin_s: f64 = std.math.inf(f64), margin_at_ms: i64 = 0, full_build_ms: i64 = 0 };

    fn deferReading(self: *GenerationProducer, tenant: []const u8, table: []const u8) DeferReading {
        var key_buf: [512]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}.{s}", .{ tenant, table }) catch return .{};
        self.cuts_lock.lock();
        defer self.cuts_lock.unlock();
        const cut = self.cuts.get(key) orelse return .{};
        return .{ .margin_s = cut.margin_s, .margin_at_ms = cut.margin_at_ms, .full_build_ms = cut.full_build_ms };
    }

    fn recordFullBuild(self: *GenerationProducer, tenant: []const u8, table: []const u8, ms: i64) void {
        var key_buf: [512]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}.{s}", .{ tenant, table }) catch return;
        self.cuts_lock.lock();
        defer self.cuts_lock.unlock();
        if (self.cuts.getPtr(key)) |cut| cut.full_build_ms = ms;
    }


    // ── §10gf: the background full lane ─────────────────────────────────────
    //
    // A full does not have to move the cut: deltas keep the cut fresh, and a full only
    // gives a client a nearer place to start. Built in the build path it did move it —
    // the pair's next delta waited for it, and (one worker) so did the edge watch — so a
    // full's build time was the chain's blind time, and under a pruning stream the hole
    // (§10ge: every remaining hole sat inside a full's build). Here the depth rotation's
    // full is built on its own thread and connections, from a snapshot taken right after
    // the pair's newest generation L, while deltas keep cutting; it is then attached to
    // L's row. A client that takes it applies every delta above L (core.planFromManifest):
    // those cover everything after L's cutoff, hence after the full's snapshot, and the
    // overlap is version-guarded upserts and deletes by key. The manifest names L's
    // cutoff for it, older than its snapshot: a watermark that only errs early.
    // Every full a correctness rule asks for (first, epoch, shape, deletes) stays in the
    // build path, from the delta's own snapshot.

    /// §10gt: what the lane is asked to build. A full carries every live row; a
    /// checkpoint carries the rows whose version moved since the last checkpoint (or the
    /// last full), tombstones included — the incremental chain's middle level.
    pub const JobKind = enum { full, checkpoint };
    pub const FullJob = struct { tenant: []const u8, table: []const u8, vcol: []const u8, tcol: []const u8, kind: JobKind = .full };

    fn pairKey(buf: []u8, tenant: []const u8, table: []const u8) ?[]const u8 {
        return std.fmt.bufPrint(buf, "{s}.{s}", .{ tenant, table }) catch null;
    }

    /// Blocks until no other build holds the pair. A sleep, not a spin: a synchronous
    /// full holds a pair for seconds.
    fn pairAcquire(self: *GenerationProducer, tenant: []const u8, table: []const u8) void {
        var buf: [512]u8 = undefined;
        const key = pairKey(&buf, tenant, table) orelse return;
        while (true) {
            self.cuts_lock.lock();
            const got = blk: {
                if (self.pair_busy.getPtr(key)) |b| {
                    if (b.*) break :blk false;
                    b.* = true;
                    break :blk true;
                }
                const owned = self.allocator.dupe(u8, key) catch break :blk true;
                self.pair_busy.put(self.allocator, owned, true) catch {
                    self.allocator.free(owned);
                };
                break :blk true;
            };
            self.cuts_lock.unlock();
            if (got) return;
            utils.sleep(5 * std.time.ns_per_ms);
        }
    }

    fn pairRelease(self: *GenerationProducer, tenant: []const u8, table: []const u8) void {
        var buf: [512]u8 = undefined;
        const key = pairKey(&buf, tenant, table) orelse return;
        self.cuts_lock.lock();
        defer self.cuts_lock.unlock();
        if (self.pair_busy.getPtr(key)) |b| b.* = false;
    }

    /// Queues the pair's full unless one is queued or building. True when queued now.
    fn requestFull(self: *GenerationProducer, tenant: []const u8, table: []const u8, vcol: []const u8, tcol: []const u8, kind: JobKind) bool {
        var buf: [512]u8 = undefined;
        const key = pairKey(&buf, tenant, table) orelse return false;
        self.cuts_lock.lock();
        defer self.cuts_lock.unlock();
        if (self.full_pending.contains(key)) return false;
        const a = self.allocator;
        const job: FullJob = .{
            .tenant = a.dupe(u8, tenant) catch return false,
            .table = a.dupe(u8, table) catch return false,
            .vcol = a.dupe(u8, vcol) catch return false,
            .tcol = a.dupe(u8, tcol) catch return false,
            .kind = kind,
        };
        const owned = a.dupe(u8, key) catch return false;
        self.full_pending.put(a, owned, {}) catch return false;
        self.full_queue.append(a, job) catch {
            _ = self.full_pending.swapRemove(key);
            return false;
        };
        return true;
    }

    fn fullLaneMain(self: *GenerationProducer) void {
        log.info("🧬 background full lane started", .{});
        while (!self.should_stop.load(.acquire)) {
            const job: ?FullJob = blk: {
                self.cuts_lock.lock();
                defer self.cuts_lock.unlock();
                if (self.full_queue.items.len == 0) break :blk null;
                break :blk self.full_queue.orderedRemove(0);
            };
            const j = job orelse {
                utils.sleep(200 * std.time.ns_per_ms);
                continue;
            };
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            self.buildFullBehind(arena.allocator(), j) catch |err| log.err("🧬 '{s}'/'{s}': background full failed: {} — the next due delta asks again", .{ j.tenant, j.table, err });
            arena.deinit();
            var buf: [512]u8 = undefined;
            if (pairKey(&buf, j.tenant, j.table)) |key| {
                self.cuts_lock.lock();
                if (self.full_pending.fetchSwapRemove(key)) |kv| self.allocator.free(kv.key);
                self.cuts_lock.unlock();
            }
            self.allocator.free(j.tenant);
            self.allocator.free(j.table);
            self.allocator.free(j.vcol);
            self.allocator.free(j.tcol);
        }
        log.info("🛑 background full lane stopped", .{});
    }

    fn buildFullBehind(self: *GenerationProducer, alloc: std.mem.Allocator, j: FullJob) !void {
        const started_ms = utils.unixMillis();
        const cs = try self.openConns(alloc);
        defer cs.close();
        const pgc = cs.pgc;
        const bkc = cs.bkc;
        var js = cs.conn_nats.jetstream(.{ .domain = self.endpoint.js_domain });
        const table = j.table;
        const tenant = j.tenant;
        const table_z = try alloc.dupeZ(u8, table);
        const tenant_z = try alloc.dupeZ(u8, tenant);
        const pair = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr };

        const cols_sel: []const u8 = blk: {
            const pub_z = try alloc.dupeZ(u8, self.publication_name);
            const params = [_]?[*:0]const u8{ pub_z.ptr, table_z.ptr };
            const res = try queryOne(pgc, "SELECT COALESCE((SELECT string_agg(quote_ident(n), ', ' ORDER BY ord) FROM unnest(pt.attnames) WITH ORDINALITY AS u(n, ord)), '*') " ++
                "FROM pg_publication_tables pt WHERE pt.pubname = $1 AND pt.tablename = $2 AND pt.schemaname = 'public'", &params);
            defer c.PQclear(res);
            break :blk if (c.PQntuples(res) > 0) try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0))) else "*";
        };
        // §10ja: every chain object is written in primary-key order. A replica's key index
        // then takes each window as an append instead of a scatter — measured in Node on
        // 3M rows, one window at a time: 115 s arriving in heap order on random keys, 21 s
        // arriving sorted — and a client need not stage and re-sort what PostgreSQL can
        // hand over in order, once per object instead of once per client.
        const order_by = try pkOrderBy(alloc, pgc, table_z);

        // ── the snapshot, taken right after the pair's newest generation ──────
        var gen_l: i64 = 0;
        var epoch_l: []const u8 = "";
        var shape_l: []const u8 = "";
        var relid_l: []const u8 = "";
        {
            self.pairAcquire(tenant, table);
            defer self.pairRelease(tenant, table);
            {
                const res = try queryOne(bkc, "SELECT gen, seed_epoch::text, COALESCE(col_shape, ''), COALESCE(relid::text, ''), " ++
                    "COALESCE((SELECT max(gen) FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND has_full AND retired_at IS NULL), 0) " ++
                    "FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND retired_at IS NULL ORDER BY gen DESC LIMIT 1", &pair);
                defer c.PQclear(res);
                if (c.PQntuples(res) == 0) return;
                gen_l = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch return;
                epoch_l = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 1)));
                shape_l = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 2)));
                relid_l = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 3)));
                const full_now = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 4)), 10) catch 0;
                if (full_now >= gen_l) return; // the newest generation already carries a full
                if (j.kind == .checkpoint) {
                    // §10gt: and it must not already carry a checkpoint either.
                    const ck = try queryOne(bkc, "SELECT COALESCE((SELECT max(gen) FROM public.zebridge_generations " ++
                        "WHERE tenant=$1 AND tbl=$2 AND has_checkpoint AND retired_at IS NULL), 0)", &pair);
                    defer c.PQclear(ck);
                    if ((std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(ck, 0, 0)), 10) catch 0) >= gen_l) return;
                }
            }
            const b = try queryOne(pgc, "BEGIN ISOLATION LEVEL REPEATABLE READ", &.{});
            c.PQclear(b);
            errdefer {
                const rb = c.PQexec(pgc, "ROLLBACK");
                c.PQclear(rb);
            }
            const tz = try queryOne(pgc, "SET LOCAL timezone TO 'UTC'", &.{});
            c.PQclear(tz);
            const set_sql = "SELECT set_config('" ++ config.Sync.tenant_setting ++ "', $1, true)";
            const tp = [_]?[*:0]const u8{tenant_z.ptr};
            const st = try queryOne(pgc, set_sql, &tp); // the first statement: the snapshot is taken here
            c.PQclear(st);
        }
        errdefer {
            const rb = c.PQexec(pgc, "ROLLBACK");
            c.PQclear(rb);
        }

        // ── content, outside the gate: deltas keep cutting meanwhile ──────────
        const shape_now: []const u8 = blk: {
            const res = try queryOne(pgc, "SELECT COALESCE(string_agg(attname || ':' || format_type(atttypid, atttypmod), ',' ORDER BY attnum), '') " ++
                "FROM pg_attribute WHERE attrelid = to_regclass('public.' || quote_ident($1)) AND attnum > 0 AND NOT attisdropped", &.{table_z.ptr});
            defer c.PQclear(res);
            break :blk try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0)));
        };
        if (shape_l.len > 0 and !std.mem.eql(u8, shape_l, shape_now)) {
            log.info("🧬 '{s}'/'{s}': background full dropped — the column shape moved since g{d}; the build path owes that full", .{ tenant, table, gen_l });
            const rb = try queryOne(pgc, "ROLLBACK", &.{});
            c.PQclear(rb);
            return;
        }
        const cutoff_l: []const u8 = blk: {
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, (try utils.allocPrintZ(alloc, "{d}", .{gen_l})).ptr };
            const res = try queryOne(bkc, "SELECT cutoff_version::text FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND gen=$3", &params);
            defer c.PQclear(res);
            if (c.PQntuples(res) == 0) return;
            break :blk try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0)));
        };
        // §10gt: what this build reads, and under which name it is published.
        //   * a FULL: every live row (the tombstoned ones are already gone from every
        //     replica — a full is applied as a wipe and a reload);
        //   * a CHECKPOINT: every row whose version moved since the last checkpoint or
        //     full, TOMBSTONES INCLUDED — that is the whole point, since a tombstoned row
        //     is how a returning client learns about a delete without reloading.
        var ckpt_lower: []const u8 = "";
        if (j.kind == .checkpoint) {
            const res = try queryOne(bkc, "SELECT COALESCE(max(cutoff_version)::text, '') FROM public.zebridge_generations " ++
                "WHERE tenant=$1 AND tbl=$2 AND (has_checkpoint OR has_full) AND retired_at IS NULL", &pair);
            defer c.PQclear(res);
            ckpt_lower = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 0)));
            if (ckpt_lower.len == 0) {
                log.info("🧬 '{s}'/'{s}': no full to checkpoint against yet — the depth rotation owes one first", .{ tenant, table });
                const rb = try queryOne(pgc, "ROLLBACK", &.{});
                c.PQclear(rb);
                return;
            }
            // ⚠️ Completeness (the plan's rule): a checkpoint holds every delete of its
            // window only while no tombstone inside it can have been reaped. The sweeper
            // reaps at `now() - threshold`, so a window reaching past that proves nothing
            // and a FULL is owed instead — said, not silently built.
            const lower_z = try alloc.dupeZ(u8, ckpt_lower);
            const gp = [_]?[*:0]const u8{lower_z.ptr};
            // ⚠️ `threshold_ms > 0` is the whole guard's precondition: the row ships with
            // 0 and the SWEEPER stamps its real retention the first time it runs. Zero
            // therefore means "nothing has ever been reaped", under which every window is
            // complete — read as "zero retention" instead, this refused every checkpoint
            // ever asked for (measured: a 20 s window rejected as past a 0 ms retention).
            const guard = queryOne(pgc, "SELECT COALESCE((SELECT max(threshold_ms) FROM public.zebridge_gc_watermark), 0) > 0 " ++
                "AND $1::timestamptz < now() - ((SELECT COALESCE(max(threshold_ms), 0) FROM public.zebridge_gc_watermark) || ' milliseconds')::interval", &gp) catch null;
            if (guard) |g| {
                defer c.PQclear(g);
                if (c.PQgetvalue(g, 0, 0)[0] == 't') {
                    log.warn("⚠️ 🧬 '{s}'/'{s}': the checkpoint's window opens at {s}, past the sweeper's tombstone retention — a delete inside it may already be reaped, so a full is owed instead of a checkpoint", .{ tenant, table, ckpt_lower });
                    const rb = try queryOne(pgc, "ROLLBACK", &.{});
                    c.PQclear(rb);
                    _ = self.requestFull(tenant, table, j.vcol, j.tcol, .full);
                    return;
                }
            }
        }
        const kind_str: []const u8 = if (j.kind == .checkpoint) "checkpoint" else "full";
        const obj_name = try std.fmt.allocPrint(alloc, "{s}-g{d}-{s}", .{ table, gen_l, if (j.kind == .checkpoint) "ckpt" else "full" });
        const sql = if (j.kind == .checkpoint) blk: {
            const lower_lit = try std.mem.replaceOwned(u8, alloc, ckpt_lower, "'", "''");
            break :blk try utils.allocPrintZ(alloc, "SELECT {s} FROM \"{s}\" WHERE \"{s}\" >= '{s}'::timestamptz{s}", .{ cols_sel, table, j.vcol, lower_lit, order_by });
        } else if (j.tcol.len > 0)
            try utils.allocPrintZ(alloc, "SELECT {s} FROM \"{s}\" WHERE \"{s}\" IS NULL{s}", .{ cols_sel, table, j.tcol, order_by })
        else
            try utils.allocPrintZ(alloc, "SELECT {s} FROM \"{s}\"{s}", .{ cols_sel, table, order_by });
        var full_rows: usize = 0;
        // §10gi: written to the store while COPY reads, inside the snapshot.
        const bucket = try std.fmt.allocPrint(alloc, "{s}{s}", .{ self.topo.generation_bucket_prefix, tenant });
        var osm = js.objectStoreManager();
        var store = try osm.openStore(bucket);
        defer store.deinit();
        const t_q = utils.unixMillis();
        const fo = try putChainObject(alloc, self.allocator, pgc, &store, obj_name, sql, gen_l, cutoff_l, j.vcol, kind_str, null);
        full_rows = fo.rows;
        const query_ms = utils.unixMillis() - t_q;
        {
            const res = try queryOne(pgc, "COMMIT", &.{});
            c.PQclear(res);
        }

        // ── attach, under the gate: still the same chain? ─────────────────────
        self.pairAcquire(tenant, table);
        defer self.pairRelease(tenant, table);
        const gen_l_z = try utils.allocPrintZ(alloc, "{d}", .{gen_l});
        const newest: struct { gen: i64, epoch: []const u8, shape: []const u8, relid: []const u8, full: i64, l_exists: bool } = blk: {
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, gen_l_z.ptr };
            const res = try queryOne(bkc, "SELECT gen, seed_epoch::text, COALESCE(col_shape, ''), COALESCE(relid::text, ''), " ++
                "COALESCE((SELECT max(gen) FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND has_full AND retired_at IS NULL), 0), " ++
                "EXISTS (SELECT 1 FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND gen=$3 AND retired_at IS NULL) " ++
                "FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND retired_at IS NULL ORDER BY gen DESC LIMIT 1", &params);
            defer c.PQclear(res);
            if (c.PQntuples(res) == 0) break :blk .{ .gen = 0, .epoch = "", .shape = "", .relid = "", .full = 0, .l_exists = false };
            break :blk .{
                .gen = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 0)), 10) catch 0,
                .epoch = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 1))),
                .shape = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 2))),
                .relid = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, 0, 3))),
                .full = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, 0, 4)), 10) catch 0,
                .l_exists = c.PQgetvalue(res, 0, 5)[0] == 't',
            };
        };
        const same_chain = newest.l_exists and newest.full < gen_l and std.mem.eql(u8, newest.epoch, epoch_l) and
            std.mem.eql(u8, newest.shape, shape_l) and std.mem.eql(u8, newest.relid, relid_l);
        if (!same_chain) {
            log.info("🧬 '{s}'/'{s}': background {s} for g{d} discarded — the chain moved while it built (newest g{d}, full g{d}, g{d} kept: {})", .{ tenant, table, kind_str, gen_l, newest.gen, newest.full, gen_l, newest.l_exists });
            store.delete(obj_name) catch {};
            return;
        }
        if (j.kind == .checkpoint) {
            const lower_z = try alloc.dupeZ(u8, ckpt_lower);
            const bytes_z = try utils.allocPrintZ(alloc, "{d}", .{fo.z_bytes});
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, gen_l_z.ptr, lower_z.ptr, bytes_z.ptr };
            const res = try queryOne(bkc, "UPDATE public.zebridge_generations SET has_checkpoint = true, ckpt_lower = $4::timestamptz, obj_bytes = $5::bigint " ++
                "WHERE tenant=$1 AND tbl=$2 AND gen=$3", &params);
            c.PQclear(res);
        } else {
            const bytes_z = try utils.allocPrintZ(alloc, "{d}", .{fo.z_bytes});
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, gen_l_z.ptr, bytes_z.ptr };
            const res = try queryOne(bkc, "UPDATE public.zebridge_generations SET has_full = true, obj_bytes = $4::bigint " ++
                "WHERE tenant=$1 AND tbl=$2 AND gen=$3", &params);
            c.PQclear(res);
        }

        // The manifest: the newest generation's own fields as they stand, the full
        // replaced, the deltas listed from the kept rows.
        const keep_from = keepFrom(newest.gen, self.chain_depth, gen_l);
        var kv = try js.kvBucket(self.topo.kv_generations);
        defer kv.deinit();
        const key = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ tenant, table });
        var entry = try kv.get(key);
        defer entry.deinit();
        const man = try std.json.parseFromSliceLeaky(std.json.Value, alloc, entry.value, .{});
        if (man != .object) return error.ManifestUnreadable;
        // Retire BEFORE rendering (see pruneChain): the row this lane attached to already
        // says has_full / has_checkpoint, so nothing is pending.
        try pruneChain(alloc, bkc, &store, tenant_z, table_z, table, keep_from, self.retire_grace_s, self.retire_windows, self.checkpoint_s > 0, 0);
        var deltas: std.json.Array = .init(alloc);
        {
            const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, (try utils.allocPrintZ(alloc, "{d}", .{keep_from})).ptr };
            const res = try queryOne(bkc, "SELECT gen, cutoff_version::text, COALESCE(prev_cutoff::text, '') " ++
                "FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND gen > $3 AND retired_at IS NULL ORDER BY gen", &params);
            defer c.PQclear(res);
            for (0..@as(usize, @intCast(c.PQntuples(res)))) |i| {
                const prev = std.mem.span(c.PQgetvalue(res, @intCast(i), 2));
                if (prev.len == 0) continue;
                const g = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, @intCast(i), 0)), 10) catch continue;
                var d: std.json.ObjectMap = .empty;
                try d.put(alloc, "gen", .{ .integer = g });
                try d.put(alloc, "object", .{ .string = try std.fmt.allocPrint(alloc, "{s}-g{d}-delta", .{ table, g }) });
                try d.put(alloc, "prev_cutoff", .{ .string = try alloc.dupe(u8, prev) });
                try d.put(alloc, "cutoff", .{ .string = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, @intCast(i), 1))) });
                try deltas.append(.{ .object = d });
            }
        }
        var root = man.object;
        if (j.kind == .full) {
            var full_obj: std.json.ObjectMap = .empty;
            try full_obj.put(alloc, "gen", .{ .integer = gen_l });
            try full_obj.put(alloc, "object", .{ .string = obj_name });
            try full_obj.put(alloc, "cutoff", .{ .string = cutoff_l });
            try root.put(alloc, "full", .{ .object = full_obj });
        }
        // §10gt: the checkpoints above the chain's full, oldest first — the middle level a
        // returning client walks instead of reloading the table (§10gs).
        const full_gen_now: i64 = if (j.kind == .full) gen_l else newest.full;
        try root.put(alloc, "checkpoints", .{ .array = try checkpointsJson(alloc, bkc, table, tenant_z, table_z, full_gen_now) });
        try root.put(alloc, "deltas", .{ .array = deltas });
        var out: std.Io.Writer.Allocating = .init(alloc);
        try std.json.Stringify.value(std.json.Value{ .object = root }, .{}, &out.writer);
        _ = try kv.put(key, out.written(), .{});

        const total_ms = utils.unixMillis() - started_ms;
        self.recordFullBuild(tenant, table, total_ms);
        log.info("🧬 '{s}'/'{s}': background {s} attached to g{d} (newest g{d}) — {d} row(s), {d} -> {d} bytes{s} in {d} ms: build (count+copy+encode+zstd+upload) {d}; deltas kept cutting meanwhile{s}", .{
            tenant, table, kind_str,                        gen_l,           newest.gen, full_rows, fo.raw_bytes, fo.z_bytes,
            if (fo.streamed) " [streamed]" else "",     total_ms,        query_ms,
            if (j.kind == .checkpoint) " (window opens at its predecessor's cutoff)" else "",
        });
    }

    /// §10ej: one pair's build with the bounded retry the post-publish check asks
    /// for: the cut fell off during the build → build again at once, three times at
    /// most. Beyond that the stream prunes faster than this pair builds — a size
    /// valve under a burst, or an age under a build — and no retry can win: say so
    /// and leave it to the next tick; clients wait for a splice rather than read past
    /// the hole (§10ei). The back-pressure that would let the WAL absorb the burst
    /// (pause publishing for one build) is the next step, not this one.
    /// Returns false when the build itself failed (the pair is unchanged in NATS);
    /// true when a generation was published, spliceable or not.
    fn buildWithRetry(self: *GenerationProducer, alloc: std.mem.Allocator, pgc: *c.PGconn, bkc: *c.PGconn, js: *nats.JetStream, table: []const u8, tenant: []const u8, vcol: []const u8, tcol: []const u8, guarded: bool, force_cut: bool) bool {
        // §10gf: one build of a pair at a time, the full lane's snapshot and attach included.
        self.pairAcquire(tenant, table);
        defer self.pairRelease(tenant, table);
        var attempt: u8 = 0;
        while (attempt < 3) : (attempt += 1) {
            var fell_off = false;
            self.buildOne(alloc, pgc, bkc, js, table, tenant, vcol, tcol, guarded, force_cut, &fell_off) catch |err| {
                log.err("🧬 generation build failed for '{s}'/'{s}': {} — next cadence retries", .{ tenant, table, err });
                return false;
            };
            if (!fell_off) return true;
            if (attempt < 2) log.warn("🧬 '{s}'/'{s}': rebuilding at once ({d}/3)", .{ tenant, table, attempt + 2 });
        }
        log.err("🧬 '{s}'/'{s}': the cut fell off the stream three builds in a row — the stream prunes faster than this pair builds (CDC_MAX_BYTES / CDC_MAX_MSGS under a burst, or CDC_MAX_AGE_SECONDS under a build); clients wait for a splice until the next cadence", .{ tenant, table });
        return true;
    }
};

/// §10ew: MessagePack written straight into a byte buffer — the five shapes a
/// chain document uses, the smallest encoding of each, as the decoders on every
/// client already accept (they read any width).
const mp = struct {
    const List = std.ArrayListUnmanaged(u8);

    fn be(out: *List, a: std.mem.Allocator, comptime T: type, v: T) !void {
        try out.appendSlice(a, &std.mem.toBytes(std.mem.nativeToBig(T, v)));
    }

    fn mapHeader(out: *List, a: std.mem.Allocator, n: usize) !void {
        if (n < 16) return out.append(a, 0x80 | @as(u8, @intCast(n)));
        try out.append(a, 0xde);
        try be(out, a, u16, @intCast(n));
    }

    fn arrayHeader(out: *List, a: std.mem.Allocator, n: usize) !void {
        if (n < 16) return out.append(a, 0x90 | @as(u8, @intCast(n)));
        if (n < 65_536) {
            try out.append(a, 0xdc);
            return be(out, a, u16, @intCast(n));
        }
        try out.append(a, 0xdd);
        try be(out, a, u32, @intCast(n));
    }

    fn str(out: *List, a: std.mem.Allocator, s: []const u8) !void {
        if (s.len < 32) {
            try out.append(a, 0xa0 | @as(u8, @intCast(s.len)));
        } else if (s.len < 256) {
            try out.appendSlice(a, &.{ 0xd9, @intCast(s.len) });
        } else if (s.len < 65_536) {
            try out.append(a, 0xda);
            try be(out, a, u16, @intCast(s.len));
        } else {
            try out.append(a, 0xdb);
            try be(out, a, u32, @intCast(s.len));
        }
        try out.appendSlice(a, s);
    }

    fn int(out: *List, a: std.mem.Allocator, v: i64) !void {
        if (v >= 0 and v < 128) return out.append(a, @intCast(v));
        if (v < 0 and v >= -32) return out.append(a, @bitCast(@as(i8, @intCast(v))));
        if (v >= -128 and v < 128) {
            try out.append(a, 0xd0);
            return out.append(a, @bitCast(@as(i8, @intCast(v))));
        }
        if (v >= -32_768 and v < 32_768) {
            try out.append(a, 0xd1);
            return be(out, a, i16, @intCast(v));
        }
        if (v >= -2_147_483_648 and v < 2_147_483_648) {
            try out.append(a, 0xd2);
            return be(out, a, i32, @intCast(v));
        }
        try out.append(a, 0xd3);
        try be(out, a, i64, v);
    }

    fn float(out: *List, a: std.mem.Allocator, f: f64) !void {
        try out.append(a, 0xcb);
        try be(out, a, u64, @bitCast(f));
    }

    /// §10ex: bytes as msgpack `bin`, the shape a bytea (or EWKB) cell takes.
    fn bin(out: *List, a: std.mem.Allocator, b: []const u8) !void {
        if (b.len < 256) {
            try out.appendSlice(a, &.{ 0xc4, @intCast(b.len) });
        } else if (b.len < 65_536) {
            try out.append(a, 0xc5);
            try be(out, a, u16, @intCast(b.len));
        } else {
            try out.append(a, 0xc6);
            try be(out, a, u32, @intCast(b.len));
        }
        try out.appendSlice(a, b);
    }
};

test "mp: the chain document's shapes decode as msgpack" {
    const a = std.testing.allocator;
    var out: mp.List = .empty;
    defer out.deinit(a);
    try mp.mapHeader(&out, a, 2);
    try mp.str(&out, a, "rows");
    try mp.arrayHeader(&out, a, 3);
    try mp.int(&out, a, -5);
    try mp.int(&out, a, 300);
    try mp.float(&out, a, 1.5);
    try mp.str(&out, a, "kind");
    try mp.str(&out, a, "x" ** 40);
    // fixmap(2) "rows" fixarray(3) negfixint(-5) int16(300) float64 "kind" str8(40)
    try std.testing.expectEqual(@as(u8, 0x82), out.items[0]);
    try std.testing.expectEqual(@as(u8, 0xa4), out.items[1]);
    try std.testing.expectEqual(@as(u8, 0x93), out.items[6]);
    try std.testing.expectEqual(@as(u8, 0xfb), out.items[7]);
    try std.testing.expectEqual(@as(u8, 0xd1), out.items[8]);
    try std.testing.expectEqual(@as(u8, 0xcb), out.items[11]);
    try std.testing.expectEqual(@as(u8, 0xd9), out.items[25]);
    try std.testing.expectEqual(@as(u8, 40), out.items[26]);
    try std.testing.expectEqual(@as(usize, 67), out.items.len);
    // bin8: 0xc4 len bytes — a bytea cell, never a string
    try mp.bin(&out, a, &.{ 0x00, 0xff, 0xfe });
    try std.testing.expectEqual(@as(u8, 0xc4), out.items[67]);
    try std.testing.expectEqual(@as(u8, 3), out.items[68]);
    try std.testing.expectEqual(@as(u8, 0xff), out.items[70]);
}

/// §10ep: `COPY (…) TO STDOUT (FORMAT binary)`, read one CopyData message at a time
/// and written as msgpack rows.
///
/// `pgoutput` in binary mode and binary COPY use the same per-type encoding —
/// PostgreSQL's send functions — so a chain row is decoded by the CDC path's own
/// decoder (`pgoutput.decodeBinColumnData`) and reaches a client as the same bytes a
/// CDC event of the same value does: integers and floats as numbers, everything else
/// as strings. A type the decoder refuses fails the read; the caller takes the text
/// path for that build (a table with such a type is suspended on CDC anyway).
const CopyReader = struct {
    pgc: *c.PGconn,
    names: []const []const u8,
    oids: []const u32,
    names_bytes: usize,
    header_seen: bool = false,
    ended: bool = false,
    nrows: usize = 0,
    /// The widest row, measured the way `wireSize` does: the chain carries any width,
    /// CDC does not (the build warns).
    widest: usize = 0,
    /// The decoder's per-value copies, reset after every row.
    scratch: std.heap.ArenaAllocator,

    fn init(alloc: std.mem.Allocator, pgc: *c.PGconn, select_sql: []const u8) !CopyReader {
        // Names and OIDs, from the same SELECT under LIMIT 0 — inside the same
        // snapshot transaction, so the shape cannot differ from the rows that follow.
        const probe_sql = try utils.allocPrintZ(alloc, "SELECT * FROM ({s}) AS q LIMIT 0", .{select_sql});
        const meta = try GenerationProducer.queryOnePub(pgc, probe_sql, &.{});
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
        defer c.PQclear(started);
        if (c.PQresultStatus(started) != c.PGRES_COPY_OUT) {
            log.err("🧬 COPY refused: {s}", .{c.PQerrorMessage(pgc)});
            return error.QueryFailed;
        }
        return .{ .pgc = pgc, .names = names, .oids = oids, .names_bytes = names_bytes, .scratch = .init(alloc) };
    }

    fn deinit(self: *CopyReader) void {
        self.scratch.deinit();
    }

    /// The rows of the next CopyData message, appended to `out`. False once the server
    /// has ended the COPY; the caller then calls `finish`.
    fn next(self: *CopyReader, out: *mp.List, oa: std.mem.Allocator) !bool {
        if (self.ended) return false;
        var buf: [*c]u8 = undefined;
        const n = c.PQgetCopyData(self.pgc, &buf, 0);
        if (n == -1) {
            self.ended = true;
            return false;
        }
        if (n < 0) {
            log.err("🧬 COPY read failed: {s}", .{c.PQerrorMessage(self.pgc)});
            return error.QueryFailed;
        }
        defer c.PQfreemem(buf);
        const data = buf[0..@intCast(n)];
        var pos: usize = 0;
        if (!self.header_seen) {
            // "PGCOPY\n\377\r\n\0", then int32 flags, then int32 extension length.
            if (data.len < 19 or !std.mem.eql(u8, data[0..11], "PGCOPY\n\xff\r\n\x00")) return error.CopyHeader;
            const ext_len: usize = @intCast(std.mem.readInt(i32, data[15..19], .big));
            pos = 19 + ext_len;
            self.header_seen = true;
        }
        const ncols = self.names.len;
        while (pos + 2 <= data.len) {
            const nfields = std.mem.readInt(i16, data[pos..][0..2], .big);
            pos += 2;
            // The trailer: no more rows, but libpq is still in COPY state until it has
            // seen the server's CopyDone — the next call reads the -1.
            if (nfields == -1) return true;
            if (@as(usize, @intCast(nfields)) != ncols) return error.CopyShape;
            _ = self.scratch.reset(.retain_capacity);
            const ra = self.scratch.allocator();
            try mp.arrayHeader(out, oa, ncols);
            var row_bytes: usize = self.names_bytes + 256; // envelope margin, mirrors wireSize
            for (0..ncols) |i| {
                const flen = std.mem.readInt(i32, data[pos..][0..4], .big);
                pos += 4;
                if (flen == -1) {
                    try out.append(oa, 0xc0);
                    continue;
                }
                const len: usize = @intCast(flen);
                const bytes = data[pos..][0..len];
                pos += len;
                row_bytes += len;
                // `owns_bytes = false`: the decoder dupes what it keeps — into the
                // row's scratch, gone with the next row.
                const v = pgoutput.decodeBinColumnData(ra, self.oids[i], bytes, false) catch |err| {
                    log.warn("🧬 COPY: column {s} (oid {d}) is not decodable by the CDC decoder ({s}) — this build takes the text path", .{ self.names[i], self.oids[i], @errorName(err) });
                    return err;
                };
                switch (v) {
                    .null, .unchanged => try out.append(oa, 0xc0),
                    .boolean => |b| try out.append(oa, if (b) 0xc3 else 0xc2),
                    .int32 => |x| try mp.int(out, oa, x),
                    .int64 => |x| try mp.int(out, oa, x),
                    .float64 => |f| try mp.float(out, oa, f),
                    .text, .numeric, .jsonb, .array => |str| try mp.str(out, oa, str),
                    .bytea => |b| try mp.bin(out, oa, b),
                }
            }
            if (row_bytes > self.widest) self.widest = row_bytes;
            self.nrows += 1;
        }
        return true;
    }

    /// Leaves the connection clean, whatever ended the read: the rest of the data is
    /// read until libpq reports the end, then the results are collected, or the next
    /// statement of this transaction fails. Returns `failed` when given.
    /// ⚠️ Unconditional and BOUNDED. Asking for results while libpq is still in COPY
    /// state hands back a COPY_OUT result every time, for ever: the first version
    /// looped there, the producer thread never returned, and the bridge's graceful stop
    /// waited on it until it was killed (measured 2026-09-11).
    fn finish(self: *CopyReader, failed: ?anyerror) !void {
        var err = failed;
        {
            var buf2: [*c]u8 = undefined;
            while (c.PQgetCopyData(self.pgc, &buf2, 0) > 0) c.PQfreemem(buf2);
        }
        var guard: u8 = 0;
        while (c.PQgetResult(self.pgc)) |r| : (guard += 1) {
            const st = c.PQresultStatus(r);
            c.PQclear(r);
            if (st == c.PGRES_COPY_OUT or guard >= 8) {
                log.err("🧬 COPY did not end cleanly (status {d} after {d} result(s)): {s}", .{ st, guard, c.PQerrorMessage(self.pgc) });
                err = err orelse error.QueryFailed;
                break;
            }
            if (err == null and st != c.PGRES_COMMAND_OK) {
                log.err("🧬 COPY ended badly: {s}", .{c.PQerrorMessage(self.pgc)});
                err = error.QueryFailed;
            }
        }
        if (err) |e| return e;
    }
};

/// A chain document's head: `{columns, rows: [` with an array32 row count, and the
/// offset of that header (patched later when the count was not known).
fn docHead(out: *mp.List, a: std.mem.Allocator, names: []const []const u8, has_prev: bool, nrows: u32) !usize {
    try mp.mapHeader(out, a, if (has_prev) 7 else 6);
    try mp.str(out, a, "columns");
    try mp.arrayHeader(out, a, names.len);
    for (names) |n| try mp.str(out, a, n);
    try mp.str(out, a, "rows");
    const at = out.items.len;
    try out.append(a, 0xdd);
    try mp.be(out, a, u32, nrows);
    return at;
}

/// A chain document's tail, after the rows: `gen, kind, cutoff, version_column, prev_cutoff?}`.
fn docTail(out: *mp.List, a: std.mem.Allocator, gen: i64, kind: []const u8, cutoff: []const u8, vcol: []const u8, prev_cutoff: ?[]const u8) !void {
    try mp.str(out, a, "gen");
    try mp.int(out, a, gen);
    try mp.str(out, a, "kind");
    try mp.str(out, a, kind);
    try mp.str(out, a, "cutoff");
    try mp.str(out, a, cutoff);
    try mp.str(out, a, "version_column");
    try mp.str(out, a, vcol);
    if (prev_cutoff) |pc| {
        try mp.str(out, a, "prev_cutoff");
        try mp.str(out, a, pc);
    }
}

/// §10gi: a full, written to the object store as it is read. Rows come from COPY in
/// batches of about 256 KiB of msgpack, go through one zstd stream, and leave as
/// object chunks: the bridge holds one batch, the compressor's window and one chunk,
/// whatever the table's size. The object store's `put` pulls through `read`.
///
/// The frame states no content size (it is not known when the header is written):
/// every client reader handles that (libzb streams it, the browser client retries
/// with a larger buffer).
///
/// Generic over the row source (`next`, `nrows`) so a test can drive it without
/// PostgreSQL; the bridge uses `CopyReader`.
fn FullStreamOf(comptime Source: type) type {
    return struct {
    const Self = @This();
    const batch: usize = 256 * 1024;
    cr: *Source,
    oa: std.mem.Allocator,
    cctx: *c.ZSTD_CCtx,
    /// Raw msgpack bytes seen — the document's size, for the logs and the ratio.
    seen: u64 = 0,
    raw: mp.List = .empty,
    raw_off: usize = 0,
    expect_rows: usize,
    gen: i64,
    /// "full", "checkpoint" or "delta" — what the document says it is (§10gt).
    kind: []const u8 = "full",
    cutoff: []const u8,
    /// A delta names where its window opens; a full and a checkpoint do not (§10gx).
    prev_cutoff: ?[]const u8 = null,
    vcol: []const u8,
    copy_done: bool = false,
    flushed: bool = false,
    /// The reader's own error, when `put` failed because of it (and not NATS).
    failed: ?anyerror = null,

    pub fn read(self: *Self, dest: []u8) anyerror!usize {
        return self.fill(dest) catch |err| {
            self.failed = err;
            return err;
        };
    }

    fn fill(self: *Self, dest: []u8) !usize {
        var out: c.ZSTD_outBuffer = .{ .dst = dest.ptr, .size = dest.len, .pos = 0 };
        while (out.pos < out.size) {
            if (self.raw_off < self.raw.items.len) {
                var in: c.ZSTD_inBuffer = .{ .src = self.raw.items.ptr, .size = self.raw.items.len, .pos = self.raw_off };
                const r = c.ZSTD_compressStream2(self.cctx, &out, &in, c.ZSTD_e_continue);
                if (c.ZSTD_isError(r) != 0) return error.ZstdCompressFailed;
                self.raw_off = in.pos;
                if (self.raw_off == self.raw.items.len) {
                    self.raw.clearRetainingCapacity();
                    self.raw_off = 0;
                }
                continue;
            }
            if (!self.copy_done) {
                while (self.raw.items.len < batch) {
                    const start = self.raw.items.len;
                    const more = try self.cr.next(&self.raw, self.oa);
                    self.seen += self.raw.items.len - start;
                    if (more) continue;
                    // The count was read in the same snapshot: a difference is a bug,
                    // and the object would carry a wrong row count.
                    if (self.cr.nrows != self.expect_rows) {
                        log.err("🧬 full stream: COPY gave {d} row(s), the snapshot's count said {d}", .{ self.cr.nrows, self.expect_rows });
                        return error.CopyCountMoved;
                    }
                    const tail = self.raw.items.len;
                    try docTail(&self.raw, self.oa, self.gen, self.kind, self.cutoff, self.vcol, self.prev_cutoff);
                    self.seen += self.raw.items.len - tail;
                    self.copy_done = true;
                    break;
                }
                continue;
            }
            if (self.flushed) break;
            var in: c.ZSTD_inBuffer = .{ .src = null, .size = 0, .pos = 0 };
            const r = c.ZSTD_compressStream2(self.cctx, &out, &in, c.ZSTD_e_end);
            if (c.ZSTD_isError(r) != 0) return error.ZstdCompressFailed;
            if (r == 0) self.flushed = true;
        }
        return out.pos;
    }
    };
}
const FullStream = FullStreamOf(CopyReader);

/// §10ja: ` ORDER BY "k1", "k2"` for the table's primary key, in index order — empty
/// when there is none (the producer only follows tables that have one).
fn pkOrderBy(alloc: std.mem.Allocator, pgc: *c.PGconn, table_z: [:0]const u8) ![]const u8 {
    const params = [_]?[*:0]const u8{table_z.ptr};
    const res = try GenerationProducer.queryOnePub(pgc, "SELECT COALESCE(string_agg(quote_ident(a.attname), ', ' ORDER BY k.ord), '') " ++
        "FROM pg_index i JOIN pg_class cl ON cl.oid = i.indrelid JOIN pg_namespace ns ON ns.oid = cl.relnamespace " ++
        "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) " ++
        "JOIN pg_attribute a ON a.attrelid = cl.oid AND a.attnum = k.attnum " ++
        "WHERE ns.nspname = 'public' AND cl.relname = $1 AND i.indisprimary", &params);
    defer c.PQclear(res);
    const cols = std.mem.span(c.PQgetvalue(res, 0, 0));
    if (cols.len == 0) return "";
    return try std.fmt.allocPrint(alloc, " ORDER BY {s}", .{cols});
}

fn compressZstd(alloc: std.mem.Allocator, src: []const u8, level: c_int) ![]u8 {
    const bound = c.ZSTD_compressBound(src.len);
    const dst = try alloc.alloc(u8, bound);
    const n = c.ZSTD_compress(dst.ptr, bound, src.ptr, src.len, level);
    if (c.ZSTD_isError(n) != 0) return error.ZstdCompressFailed;
    return dst[0..n];
}




/// Prunes a chain's generations at or below `keep_from`: PostgreSQL rows first (the
/// authority), then their objects.
/// §10gq: retire, then delete. A generation at or below the kept window leaves the
/// manifest AT ONCE — no new client can plan from it — but its objects stay for
/// `grace_s`, because a client may be READING them: a seed of a 17M-row full takes about
/// 190 s while, at 100k events a second, a background full replaced the last one every
/// ~45 s. Measured before this: two seeds died with ObjectNotFound mid-read, and the
/// client had to start over (§10gm).
///
/// ⚠️ TWO bounds, and both are needed. Keeping only the newest retired window (the first
/// version) cancelled the grace outright: a background full attaches every ~45 s under the
/// firehose, so each retirement deleted the last and a 190 s seed still lost its objects.
/// Keeping everything within the grace (the second) filled the disk — thirteen fulls at a
/// 600 s grace, nats-server logging "Critical write error: no space", seeds failing for
/// want of a store rather than an object. So: the newest `keep_windows` retirements, none
/// older than `grace_s`. The protection a client actually gets is whichever binds first
/// — min(grace, windows x replacement interval) — and the storage is bounded at
/// `keep_windows` fulls. A pair that replaces its full every few seconds is the case
/// incremental fulls remove: a base is replaced rarely, and then the grace alone binds.
/// `pending_full_gen`: the generation whose FULL this cut is publishing but has not yet
/// recorded (the row lands after the manifest, §10gi) — 0 when the cut carries none.
///
/// ⚠️ Called BEFORE the manifest is rendered, not after. Rendering first and retiring
/// second left the manifest naming a generation that had just been retired, until the
/// next cut rewrote it — on a quiet table, indefinitely; measured 2026-09-17 as
/// `users-g4-delta` still in the manifest with g4 retired. Retiring first is safe in
/// the other direction: a retired generation keeps its objects for the grace, so the
/// previous manifest (live for the milliseconds until the put) still resolves.
fn pruneChain(alloc: std.mem.Allocator, bkc: *c.PGconn, store: anytype, tenant_z: [:0]const u8, table_z: [:0]const u8, table: []const u8, keep_from: i64, grace_s: u64, keep_windows: u32, by_levels: bool, pending_full_gen: i64) !void {
    if (by_levels) {
        // §10gw: retention by the chain's LEVELS, not by counting generations.
        //   * the newest base is kept, and everything older than it goes;
        //   * checkpoints are kept from that base on — they are how a returning client
        //     walks the distance the deltas no longer cover;
        //   * a delta is kept only above the newest checkpoint: below it the checkpoint
        //     carries the same rows in one object.
        // ⚠️ A pair whose lane has STALLED keeps its deltas: with no checkpoint above the
        // base, `ckpt` IS the base and nothing between them is pruned. That is the safe
        // direction — pruning deltas down to a checkpoint that never came would strand
        // every returning client on the base.
        const pending_z = try utils.allocPrintZ(alloc, "{d}", .{pending_full_gen});
        const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, pending_z.ptr };
        const res = try GenerationProducer.queryOnePub(bkc,
            "WITH base AS (SELECT GREATEST(COALESCE(max(gen), 0), $3::bigint) AS gen FROM public.zebridge_generations " ++
            "              WHERE tenant=$1 AND tbl=$2 AND has_full AND retired_at IS NULL), " ++
            "     ckpt AS (SELECT COALESCE(max(gen), (SELECT gen FROM base)) AS gen FROM public.zebridge_generations " ++
            "              WHERE tenant=$1 AND tbl=$2 AND has_checkpoint AND retired_at IS NULL " ++
            "                AND gen >= (SELECT gen FROM base)) " ++
            "UPDATE public.zebridge_generations g SET retired_at = now() " ++
            "WHERE g.tenant=$1 AND g.tbl=$2 AND g.retired_at IS NULL AND (SELECT gen FROM base) > 0 " ++
            "  AND (g.gen < (SELECT gen FROM base) " ++
            "       OR (g.gen < (SELECT gen FROM ckpt) AND NOT g.has_checkpoint AND g.gen <> (SELECT gen FROM base)))", &params);
        c.PQclear(res);
    } else if (keep_from > 0) {
        const keep_z = try utils.allocPrintZ(alloc, "{d}", .{keep_from});
        const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, keep_z.ptr };
        const res = try GenerationProducer.queryOnePub(bkc, "UPDATE public.zebridge_generations SET retired_at = now() " ++
            "WHERE tenant=$1 AND tbl=$2 AND gen <= $3 AND retired_at IS NULL", &params);
        c.PQclear(res);
    } else return;
    // Gone: past the grace, or superseded by a newer retirement (the one-window bound).
    const grace_z = try utils.allocPrintZ(alloc, "{d}", .{grace_s});
    const windows_z = try utils.allocPrintZ(alloc, "{d}", .{keep_windows});
    const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, grace_z.ptr, windows_z.ptr };
    const res = try GenerationProducer.queryOnePub(bkc, "DELETE FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND retired_at IS NOT NULL " ++
        "AND (retired_at < now() - ($3 || ' seconds')::interval " ++
        "     OR retired_at NOT IN (SELECT DISTINCT retired_at FROM public.zebridge_generations " ++
        "                           WHERE tenant=$1 AND tbl=$2 AND retired_at IS NOT NULL ORDER BY retired_at DESC LIMIT $4)) " ++
        "RETURNING gen", &params);
    defer c.PQclear(res);
    const pruned: usize = @intCast(c.PQntuples(res));
    if (pruned == 0) return;
    for (0..pruned) |i| {
        const g = std.mem.span(c.PQgetvalue(res, @intCast(i), 0));
        // §10gu: "ckpt" among them — a pruned checkpoint's object was left in the store
        // for ever, since only the delta and the full were ever named here. "dict": the
        // dictionary object eras cut before 2026-09-24 kept per full — gone with its row.
        for ([_][]const u8{ "delta", "full", "ckpt", "dict" }) |kind| {
            const old_name = try std.fmt.allocPrint(alloc, "{s}-g{s}-{s}", .{ table, g, kind });
            store.delete(old_name) catch |err| {
                if (err != error.ObjectNotFound) log.warn("🧬 could not delete pruned object {s}: {}", .{ old_name, err });
            };
        }
    }
}

/// §10gt: the manifest's `checkpoints`, oldest first: every generation above the chain's
/// full that carries one. `lower` is the window's start — what a returning client compares
/// its watermark against before deciding it can skip the base (§10gs).
fn checkpointsJson(alloc: std.mem.Allocator, bkc: *c.PGconn, table: []const u8, tenant_z: [:0]const u8, table_z: [:0]const u8, full_gen: i64) !std.json.Array {
    var out: std.json.Array = .init(alloc);
    const gen_z = try utils.allocPrintZ(alloc, "{d}", .{full_gen});
    const params = [_]?[*:0]const u8{ tenant_z.ptr, table_z.ptr, gen_z.ptr };
    const res = GenerationProducer.queryOnePub(bkc, "SELECT gen, cutoff_version::text, COALESCE(ckpt_lower::text, '') " ++
        "FROM public.zebridge_generations WHERE tenant=$1 AND tbl=$2 AND has_checkpoint AND retired_at IS NULL AND gen > $3 ORDER BY gen", &params) catch return out;
    defer c.PQclear(res);
    for (0..@as(usize, @intCast(c.PQntuples(res)))) |i| {
        const g = std.fmt.parseInt(i64, std.mem.span(c.PQgetvalue(res, @intCast(i), 0)), 10) catch continue;
        var ck: std.json.ObjectMap = .empty;
        try ck.put(alloc, "gen", .{ .integer = g });
        try ck.put(alloc, "object", .{ .string = try std.fmt.allocPrint(alloc, "{s}-g{d}-ckpt", .{ table, g }) });
        try ck.put(alloc, "lower", .{ .string = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, @intCast(i), 2))) });
        try ck.put(alloc, "cutoff", .{ .string = try alloc.dupe(u8, std.mem.span(c.PQgetvalue(res, @intCast(i), 1))) });
        try out.append(.{ .object = ck });
    }
    return out;
}

/// §10ge: the depth rotation's full, now or later. Pure, for the tests.
///   * `since`: generations since the last full, this one included; `limit`: the bound;
///   * the margin: seconds left before the stream prunes past the pair's cut, read by
///     the edge watch `age_ms` ago (`read` false: never read);
///   * `full_ms`: the pair's last full build (0: none seen, nothing to weigh against).
/// It waits only on a FRESH reading (within one slow scan and two seconds) that says
/// the margin is under three full builds plus one scan: the same weighing as the edge
/// watch's early cut, applied to the build that costs the most.
pub const FullDecision = enum { build, defer_full, build_at_limit };

/// §10gl: the SQL expression a delta's rows must be at or above (`version >= …`), as
/// literals: COPY takes no parameters. `cut` and `floor` are PostgreSQL's own renderings
/// of timestamptz values from the bookkeeping row, quoted all the same.
///
/// - `floor` known, table not written by the edge: the floor. Every row a later commit
///   makes visible carries a version at or above it (the oldest open transaction's start,
///   or the cut), so nothing is re-read that the previous delta already carried, except
///   the rows of transactions open across the cut.
/// - `floor` known, edge-writable: the lower of the floor and the cut minus the version
///   tolerance, since a client's version may trail the database clock by that much.
/// - `floor` unknown (a row from before the column): the cut minus the tolerance, as before.
pub fn deltaLowerBound(alloc: std.mem.Allocator, cut: []const u8, floor: ?[]const u8, edge_writable: bool) ![]const u8 {
    const cut_lit = try std.mem.replaceOwned(u8, alloc, cut, "'", "''");
    const tol = config.Sync.version_future_tolerance;
    const f = floor orelse return std.fmt.allocPrint(alloc, "'{s}'::timestamptz - interval '{s}'", .{ cut_lit, tol });
    const floor_lit = try std.mem.replaceOwned(u8, alloc, f, "'", "''");
    if (!edge_writable) return std.fmt.allocPrint(alloc, "'{s}'::timestamptz", .{floor_lit});
    return std.fmt.allocPrint(alloc, "LEAST('{s}'::timestamptz - interval '{s}', '{s}'::timestamptz)", .{ cut_lit, tol, floor_lit });
}

pub fn fullDecision(enabled: bool, since: i64, limit: i64, margin_s: f64, age_ms: i64, read: bool, full_ms: i64, scan_s: u64) FullDecision {
    if (!enabled or !read or full_ms <= 0) return .build;
    if (age_ms > @as(i64, @intCast(scan_s)) * 1000 + 2000) return .build;
    const need_s = 3.0 * @as(f64, @floatFromInt(full_ms)) / 1000.0 + @as(f64, @floatFromInt(scan_s));
    if (!(margin_s < need_s)) return .build;
    return if (since >= limit) .build_at_limit else .defer_full;
}

/// §10ge: the generations kept (pruning) and listed (manifest) are those ABOVE this:
/// the last `depth`, stretched back to keep the newest full and every delta after it.
/// `newest_full` is this generation when it carries a full.
pub fn keepFrom(gen: i64, depth: u32, newest_full: i64) i64 {
    const by_depth = gen - @as(i64, depth);
    if (newest_full <= 0) return @max(by_depth, 0);
    return @max(@min(by_depth, newest_full - 1), 0);
}

test "fullDecision: builds unless a fresh, short margin says wait; the limit wins (§10ge)" {
    // margin 5 s, full 2 s → need 3×2 + 5 = 11 s: defer
    try std.testing.expectEqual(FullDecision.defer_full, fullDecision(true, 5, 23, 5.0, 1000, true, 2000, 5));
    // same, at the limit
    try std.testing.expectEqual(FullDecision.build_at_limit, fullDecision(true, 23, 23, 5.0, 1000, true, 2000, 5));
    // plenty of margin
    try std.testing.expectEqual(FullDecision.build, fullDecision(true, 5, 23, 60.0, 1000, true, 2000, 5));
    // no pruning (infinite margin)
    try std.testing.expectEqual(FullDecision.build, fullDecision(true, 5, 23, std.math.inf(f64), 1000, true, 2000, 5));
    // a stale reading, never read, no full measured, or turned off
    try std.testing.expectEqual(FullDecision.build, fullDecision(true, 5, 23, 5.0, 60_000, true, 2000, 5));
    try std.testing.expectEqual(FullDecision.build, fullDecision(true, 5, 23, 5.0, 1000, false, 2000, 5));
    try std.testing.expectEqual(FullDecision.build, fullDecision(true, 5, 23, 5.0, 1000, true, 0, 5));
    try std.testing.expectEqual(FullDecision.build, fullDecision(false, 5, 23, 5.0, 1000, true, 2000, 5));
}

test "keepFrom: the last depth, stretched back to the newest full (§10ge)" {
    // no deferral: full at gen 10, gen 12, depth 6 → keep > 6
    try std.testing.expectEqual(@as(i64, 6), keepFrom(12, 6, 10));
    // deferred: gen 20, last full 10 → keep > 9 (the full and every delta after it)
    try std.testing.expectEqual(@as(i64, 9), keepFrom(20, 6, 10));
    // this generation carries the full → the plain window
    try std.testing.expectEqual(@as(i64, 14), keepFrom(20, 6, 20));
    // young chains keep everything
    try std.testing.expectEqual(@as(i64, 0), keepFrom(3, 6, 1));
    try std.testing.expectEqual(@as(i64, 0), keepFrom(8, 6, 1));
}

test "FullStreamOf: one zstd frame with no content size, the document intact, in chunk-sized reads (§10gi)" {
    const a = std.testing.allocator;
    const Rows = struct {
        nrows: usize = 0,
        total: usize,
        fn next(self: *@This(), out: *mp.List, oa: std.mem.Allocator) !bool {
            if (self.nrows == self.total) return false;
            // A few rows per call, like a CopyData message.
            var i: usize = 0;
            while (i < 3 and self.nrows < self.total) : (i += 1) {
                try mp.arrayHeader(out, oa, 2);
                try mp.int(out, oa, @intCast(self.nrows));
                try mp.str(out, oa, "some text that repeats a little, row after row");
                self.nrows += 1;
            }
            return true;
        }
    };
    const total = 50_000;
    var rows: Rows = .{ .total = total };
    const cctx = c.ZSTD_createCCtx().?;
    defer _ = c.ZSTD_freeCCtx(cctx);
    _ = c.ZSTD_CCtx_setParameter(cctx, c.ZSTD_c_compressionLevel, 3);
    var fs: FullStreamOf(Rows) = .{ .cr = &rows, .oa = a, .cctx = cctx, .expect_rows = total, .gen = 7, .cutoff = "2026-09-15 00:00:00+00", .vcol = "updated_at" };
    defer fs.raw.deinit(a);
    const names = [_][]const u8{ "id", "note" };
    _ = try docHead(&fs.raw, a, &names, false, total);
    fs.seen += fs.raw.items.len; // the head, as putChainObject counts it

    var z: std.ArrayListUnmanaged(u8) = .empty;
    defer z.deinit(a);
    var chunk: [4096]u8 = undefined;
    while (true) {
        const n = try fs.read(&chunk);
        if (n == 0) break;
        // Every read but the last fills its chunk: an object store publishes one per read.
        try z.appendSlice(a, chunk[0..n]);
        if (n < chunk.len) try std.testing.expectEqual(@as(usize, 0), try fs.read(&chunk));
        if (n < chunk.len) break;
    }
    const unknown: c_ulonglong = std.math.maxInt(c_ulonglong);
    try std.testing.expectEqual(unknown, c.ZSTD_getFrameContentSize(z.items.ptr, z.items.len));

    // The document, as a reader without the size decodes it.
    var in: std.Io.Reader = .fixed(z.items);
    var out: std.Io.Writer.Allocating = .init(a);
    defer out.deinit();
    var zs: std.compress.zstd.Decompress = .init(&in, &.{}, .{});
    _ = try zs.reader.streamRemaining(&out.writer);
    const doc = out.written();
    try std.testing.expectEqual(fs.seen, doc.len);
    // {columns, rows: array32(total) …, gen, kind, cutoff, version_column}
    try std.testing.expectEqual(@as(u8, 0x86), doc[0]);
    const rows_at = std.mem.indexOf(u8, doc, "\xa4rows").? + 5;
    try std.testing.expectEqual(@as(u8, 0xdd), doc[rows_at]);
    try std.testing.expectEqual(@as(u32, total), std.mem.readInt(u32, doc[rows_at + 1 ..][0..4], .big));
    try std.testing.expect(std.mem.endsWith(u8, doc, "\xaeversion_column\xaaupdated_at"));
}

test "deltaLowerBound: the floor alone, the tolerance only where the edge writes or nothing was recorded (§10gl)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cut = "2026-09-15 18:49:53.04047+00";
    const floor = "2026-09-15 18:49:52.9+00";
    try std.testing.expectEqualStrings("'2026-09-15 18:49:52.9+00'::timestamptz", try deltaLowerBound(a, cut, floor, false));
    try std.testing.expectEqualStrings("LEAST('2026-09-15 18:49:53.04047+00'::timestamptz - interval '5 seconds', '2026-09-15 18:49:52.9+00'::timestamptz)", try deltaLowerBound(a, cut, floor, true));
    try std.testing.expectEqualStrings("'2026-09-15 18:49:53.04047+00'::timestamptz - interval '5 seconds'", try deltaLowerBound(a, cut, null, false));
    // quoted, never closed
    try std.testing.expectEqualStrings("'x'' OR 1=1 --'::timestamptz", try deltaLowerBound(a, cut, "x' OR 1=1 --", false));
}
