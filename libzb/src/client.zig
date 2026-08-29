//! The orchestration loop — the third piece after core and the two shells.
//!
//! Wires core.zig (decisions) to storage.zig (SQLite) and transport.zig
//! (nats.zig) into a working read-path client: schemas from KV, seeding from
//! generation chains (with the §10n destruction guard), the per-stream gap
//! rule, the finding-7/10 seed gate, FK hold/retry, and durable positions.
//! This is INCREMENT 1: read path + catch-up-to-tail semantics. The write
//! path (outbox, verdicts, HLC stamping via core) and live tailing are next.
//!
//! Composition, not invention: every rule here is a call into core.zig, the
//! same functions the 91 fixtures pin.

const std = @import("std");
const core = @import("core.zig");
const storage = @import("storage.zig");
const transport = @import("transport.zig");
const msgpack = @import("msgpack");
const C = @import("c");

const Value = std.json.Value;

pub const Options = struct {
    url: []const u8,
    creds_path: []const u8,
    grammar_path: []const u8,
    db_path: [*:0]const u8,
    principal: []const u8,
    /// Parents FIRST: seeding runs in this order with foreign_keys ON.
    tables: []const []const u8,
    /// This replica's identity, and it must be STABLE across restarts: it is the
    /// tiebreak value the bridge stores and the prefix of every msg_id, so a client
    /// that changes it loses idempotency on anything still unconfirmed.
    client_id: []const u8 = "zig-client",
};

const TableState = struct {
    pk: []const []const u8,
    cols: []const []const u8,
    version_col: ?[]const u8,
    tenant_col: ?[]const u8,
    /// PROTOCOL §7.5: a soft delete arrives as an update setting this column, and the
    /// physical reap is never forwarded — so a row with it set is REMOVED here, on
    /// seed, on CDC and on the local optimistic apply alike. See `tombstoned`.
    tombstone_col: ?[]const u8,
    route: []const u8, // the CDC stream this table's events ride
    seed_seq: ?u64 = null,
    seed_stream: ?[]const u8 = null,
    seed_lsn: ?i64 = null,
};

const Held = struct { table: []const u8, ev: Value };

pub const SyncClient = struct {
    a: std.mem.Allocator,
    arena: std.heap.ArenaAllocator, // client-lifetime allocations (states, grammar)
    t: *transport.Transport,
    st: storage.Storage,
    opts: Options,

    // ── grammar.json, and ONLY grammar.json (PROTOCOL §1) ─────────────────────
    // No defaults: every name below is REQUIRED by `loadGrammar`, which fails naming
    // the missing key. A hardcoded fallback would let a renamed stream or bucket
    // leave this client publishing into, or watching, a subject nobody else uses —
    // with no error on either side. `undefined` until `loadGrammar` has run; `init`
    // does not return without it.
    cdc_prefix: []const u8 = undefined, // cdc_streams.tenant_prefix — the CDC_<tenant> STREAM prefix
    cdc_public: []const u8 = undefined, // cdc_streams.public
    kv_schemas: []const u8 = undefined, // kv.schemas
    kv_tenants: []const u8 = undefined, // kv.tenants
    kv_generations: []const u8 = undefined, // generations.kv
    gen_bucket_prefix: []const u8 = undefined, // generations.bucket_prefix
    open_tenant: []const u8 = undefined, // open_tenant
    stream_mutations: []const u8 = undefined, // streams.mutations
    subject_mutations_prefix: []const u8 = undefined, // subjects.mutations_prefix
    subject_mutation_ack_prefix: []const u8 = undefined, // subjects.mutation_ack_prefix

    tenant: []const u8 = "",
    /// §10x dictionaries by object name — immutable, so the cache cannot go stale.
    dicts: std.StringArrayHashMapUnmanaged([]const u8) = .empty,
    states: std.StringArrayHashMapUnmanaged(TableState) = .empty,
    /// FK-held events (§10h) outlive the `drainStream` call that decoded them — they
    /// wait for `drainCdc`'s retry pass — but not the pass itself. So they get their
    /// own arena, reset after every pass: a third lifetime, between "this call" and
    /// "this client", and the only one that needs a deep copy (see `cloneValue`).
    held_arena: std.heap.ArenaAllocator,
    held: std.ArrayList(Held) = .empty,
    /// The HLC's two inputs (§7.2). `seen_floor` is the newest version this replica
    /// has OBSERVED (from CDC), `last_version` its own last stamp. A version is
    /// strictly after both, so a lagging clock cannot stamp under a row it has seen —
    /// and arrival time never becomes the comparator, which would punish offline edits.
    ///
    /// ⚠️ `last_version` is a slice into `last_version_buf`, not an arena string: it is
    /// rewritten on every `mutate`, and a per-write dupe into the client arena would
    /// grow that arena for the life of the client (the pattern §1 of the review found
    /// in every loop entry point — the arena is a LIFETIME, not a convenience).
    seen_floor: []const u8 = "",
    seen_floor_buf: [64]u8 = undefined,
    last_version: []const u8 = "",
    last_version_buf: [64]u8 = undefined,
    /// Held open across mutate/flush: a CORE subscription only delivers what arrives
    /// while it exists, so subscribing after publishing misses the verdict every time
    /// (measured — the demo settled 0 of 1 until this was split out of drainVerdicts).
    verdicts: ?*@import("nats").Subscription = null,

    /// Acquire in order, and register the matching release BEFORE the next acquire.
    ///
    /// ⚠️ This used to leak on every failure path, and the shape is worth naming
    /// because it reads as correct: `Storage.open` sat INSIDE the struct literal while
    /// `errdefer a.destroy(self)` came AFTER it, so a database that would not open
    /// leaked the struct — and a failure in `loadGrammar` or `connect` destroyed the
    /// struct while leaving the SQLite handle open and the arena unfreed. A leaked DB
    /// handle is precisely what bites a host that retries `open` after a failure.
    ///
    /// The rule that prevents all three: one acquire per statement, its `errdefer` on
    /// the next line, in the same order `deinit` releases them in reverse.
    pub fn init(a: std.mem.Allocator, opts: Options) !*SyncClient {
        const self = try a.create(SyncClient);
        errdefer a.destroy(self);

        self.* = .{
            .a = a,
            .arena = std.heap.ArenaAllocator.init(a),
            .held_arena = std.heap.ArenaAllocator.init(a),
            .t = undefined,
            .st = undefined,
            .opts = opts,
        };
        errdefer self.arena.deinit();
        errdefer self.held_arena.deinit();

        self.st = try storage.Storage.open(opts.db_path);
        errdefer self.st.close();

        self.t = try transport.Transport.connect(a, .{ .url = opts.url, .creds_path = opts.creds_path });
        errdefer self.t.deinit();

        // After the transport: the grammar read borrows its Io rather than standing up
        // a second thread pool for one file.
        try self.loadGrammar();

        return self;
    }

    /// Release EVERY handle, in the exact reverse of `init`'s acquisition order.
    ///
    /// ⚠️ The subscription must go before the transport: nats.zig panics if a
    /// connection is destroyed with a live subscription, and it is right to — a
    /// subscription is owned by its creator, so cascading the destroy would leave the
    /// caller holding a pointer into freed state, which is a use-after-free later and
    /// somewhere unrelated. The panic reports the mistake where it is made.
    ///
    /// That panic found a real hole here: `verdicts` is the only handle this client
    /// holds ACROSS calls, and it was added as a field without being added to this
    /// function. `releasables()` below exists so the next such field cannot be
    /// forgotten the same way — it is the one list, and both this and the test read it.
    pub fn deinit(self: *SyncClient) void {
        self.releaseHandles();
        self.st.close();
        self.held_arena.deinit(); // `held`'s backing array lives here too
        self.arena.deinit();
        self.a.destroy(self);
    }

    /// Everything acquired after the transport, released before it — in ONE place, so
    /// that adding a handle has an obvious home and cannot be forgotten the way
    /// `verdicts` was. (Only `deinit` calls it today; a reconnect that had to release
    /// and re-acquire this set would call the same function.)
    fn releaseHandles(self: *SyncClient) void {
        if (self.verdicts) |sub| {
            sub.deinit();
            self.verdicts = null;
        }
        self.t.deinit();
    }

    fn aa(self: *SyncClient) std.mem.Allocator {
        return self.arena.allocator();
    }

    fn loadGrammar(self: *SyncClient) !void {
        // Client-lifetime arena, deliberately: the grammar strings below are read for
        // the life of the client, and this runs once.
        const a = self.aa();
        const io = self.t.threaded.io();
        const bytes = try std.Io.Dir.cwd().readFileAlloc(io, self.opts.grammar_path, a, .limited(1 << 20));
        const g = try std.json.parseFromSlice(Value, a, bytes, .{});
        const root = g.value;
        self.cdc_prefix = try grammarString(root, &.{ "cdc_streams", "tenant_prefix" });
        self.cdc_public = try grammarString(root, &.{ "cdc_streams", "public" });
        self.kv_schemas = try grammarString(root, &.{ "kv", "schemas" });
        self.kv_tenants = try grammarString(root, &.{ "kv", "tenants" });
        self.kv_generations = try grammarString(root, &.{ "generations", "kv" });
        self.gen_bucket_prefix = try grammarString(root, &.{ "generations", "bucket_prefix" });
        self.open_tenant = try grammarString(root, &.{"open_tenant"});
        self.stream_mutations = try grammarString(root, &.{ "streams", "mutations" });
        self.subject_mutations_prefix = try grammarString(root, &.{ "subjects", "mutations_prefix" });
        self.subject_mutation_ack_prefix = try grammarString(root, &.{ "subjects", "mutation_ack_prefix" });
    }

    // ─── step 0: tenant (PROTOCOL §6 Step 0 — resolved, never guessed) ──────

    pub fn resolveTenant(self: *SyncClient) !void {
        const bytes = (try self.t.kvGet(self.aa(), self.kv_tenants, self.opts.principal)) orelse {
            self.tenant = self.open_tenant;
            return;
        };
        // The value may be msgpack-encoded (the bridge's KV diversion) or raw.
        self.tenant = decodeMaybeMsgpackString(self.aa(), bytes) catch bytes;
        std.debug.print("tenant: {s} -> {s}\n", .{ self.opts.principal, self.tenant });
    }

    // ─── step 1: schemas → local DDL, existence from the DATABASE (finding 9) ─

    pub fn syncSchemas(self: *SyncClient) !void {
        const a = self.aa();
        for (self.opts.tables) |table| {
            const bytes = (try self.t.kvGet(a, self.kv_schemas, table)) orelse {
                std.debug.print("schema missing for {s}\n", .{table});
                continue;
            };
            const parsed = try std.json.parseFromSlice(Value, a, bytes, .{});
            const val = parsed.value;

            const pk = try jsonStrList(a, val.object.get("pk_columns"));
            const cols_v = val.object.get("sqlite").?.object.get("columns").?;
            var names: std.ArrayList([]const u8) = .empty;
            for (cols_v.array.items) |c| try names.append(a, c.object.get("name").?.string);
            const tenant_col: ?[]const u8 = if (val.object.get("tenant_column")) |v| (if (v == .string) v.string else null) else null;
            const version_col: ?[]const u8 = if (val.object.get("version_column")) |v| (if (v == .string) v.string else null) else null;
            const tombstone_col: ?[]const u8 = if (val.object.get("tombstone_column")) |v| (if (v == .string) v.string else null) else null;

            // FINDING 9: existence is the DATABASE's to answer, never a map's.
            var qa = std.heap.ArenaAllocator.init(self.a);
            defer qa.deinit();
            const exists = (try self.st.query(qa.allocator(),
                "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", &.{.{ .text = table }})).len > 0;
            if (!exists) {
                const fks = val.object.get("foreign_keys") orelse Value{ .array = std.json.Array.init(a) };
                const steps = try core.createTableSteps(a, table, cols_v.array, pk, fks.array);
                for (steps.array.items) |stp| try self.execStep(stp);
                const idx = val.object.get("indexes") orelse Value{ .array = std.json.Array.init(a) };
                const plan = try core.indexSyncPlan(a, table, &.{}, idx.array);
                for (plan.object.get("creates").?.array.items) |stp| try self.execStep(stp);
                std.debug.print("{s}: created\n", .{table});
            }
            // TODO increment 2: the alter/rebuild path via core.diffColumns —
            // this increment assumes a stable schema during the run.

            const route = if (tenant_col != null)
                try std.fmt.allocPrint(a, "{s}{s}", .{ self.cdc_prefix, self.tenant })
            else
                self.cdc_public;
            try self.states.put(a, table, .{
                .pk = pk,
                .cols = names.items,
                .version_col = version_col,
                .tenant_col = tenant_col,
                .tombstone_col = tombstone_col,
                .route = route,
            });
        }
        try self.st.execSimple("CREATE TABLE IF NOT EXISTS _zbz_stream_seq (stream TEXT PRIMARY KEY, last_seq INTEGER NOT NULL)");
        try self.st.execSimple("CREATE TABLE IF NOT EXISTS _zbz_generations (tbl TEXT PRIMARY KEY, watermark TEXT, cutoff_lsn INTEGER)");
    }

    /// DDL steps from core carry no params (unlike the DML plans `stepExec` runs).
    fn execStep(self: *SyncClient, stp: Value) !void {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        _ = try self.st.query(qa.allocator(), stp.object.get("sql").?.string, &.{});
    }

    // ─── positions ──────────────────────────────────────────────────────────

    /// ⚠️ A read failure is an ERROR, not 0. Answering 0 to "where was I" tells the
    /// gap rule the replica has never been here, which is a full re-seed — the most
    /// expensive thing this client can do, triggered by a locked database file.
    fn storedSeq(self: *SyncClient, stream: []const u8) !u64 {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        const rows = try self.st.query(qa.allocator(),
            "SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", &.{.{ .text = stream }});
        if (rows.len == 0) return 0;
        return @intCast(rows[0][0].integer);
    }

    fn persistSeq(self: *SyncClient, stream: []const u8, seq: u64) !void {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        _ = try self.st.query(qa.allocator(),
            "INSERT INTO _zbz_stream_seq (stream, last_seq) VALUES (?, ?) ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq",
            &.{ .{ .text = stream }, .{ .integer = @intCast(seq) } });
    }

    fn effTenant(self: *SyncClient, table: []const u8) []const u8 {
        const st = self.states.get(table) orelse return self.open_tenant;
        return if (st.tenant_col != null) self.tenant else self.open_tenant;
    }

    // ─── step 2: the gap rule (per stream) + scoped seeding (§10n) ──────────

    pub fn gapAndSeed(self: *SyncClient) !void {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        var gapped: std.StringArrayHashMapUnmanaged(void) = .empty;
        var it = self.states.iterator();
        while (it.next()) |e| {
            const stream = e.value_ptr.route;
            if (gapped.contains(stream)) continue;
            var info = self.t.js.getStreamInfo(stream) catch continue;
            defer info.deinit();
            const first: i64 = @intCast(info.value.state.first_seq);
            const stored: i64 = @intCast(try self.storedSeq(stream));
            if (core.streamHasGap(first, stored)) try gapped.put(a, stream, {});
        }
        // Seed in the CONFIGURED order (parents first) with foreign_keys ON:
        // scoped to gapped routes plus never-seeded tables (§10n).
        for (self.opts.tables) |table| {
            const st = self.states.get(table) orelse continue;
            // ⚠️ `try`, not "treat a failed read as never seeded": that would answer a
            // locked database with a full re-seed (the same trap as `storedSeq`).
            const seeded = (try self.st.query(a,
                "SELECT tbl FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }})).len > 0;
            if (gapped.contains(st.route) or !seeded) {
                try self.applyChain(table);
            }
        }
    }

    /// One chain step's rows, applied inside `Storage.transaction` — the DELETE of a
    /// full shares the transaction so a crash mid-apply cannot leave an empty table
    /// (§10n). A struct because `transaction` takes `(ctx, fn)`, and the fn needs
    /// everything the step decoded.
    const ChainStep = struct {
        client: *SyncClient,
        a: std.mem.Allocator,
        table: []const u8,
        sql: []const u8,
        rows: std.json.Array,
        is_full: bool,
        cols: []const []const u8,
        pk: []const []const u8,
        /// Index of the tombstone column in `cols`, when the table has one and the
        /// chain carries it.
        tomb_idx: ?usize,

        fn apply(cs: ChainStep, st: *storage.Storage) !void {
            if (cs.is_full) {
                const del = try std.fmt.allocPrint(cs.a, "DELETE FROM {s}", .{cs.table});
                _ = try st.query(cs.a, del, &.{});
            }
            for (cs.rows.items, 0..) |row, n| {
                // §7.5: a tombstoned row in a chain is a row this replica must NOT hold.
                // A chain is built from the table as it stands, so it carries every
                // tombstone not yet reaped; the reap itself never reaches a client.
                if (cs.tomb_idx) |ti| if (ti < row.array.items.len and row.array.items[ti] != .null) {
                    var keyed: std.json.ObjectMap = .empty;
                    for (cs.pk) |pc| {
                        for (cs.cols, 0..) |c, i| if (std.mem.eql(u8, c, pc) and i < row.array.items.len) {
                            try keyed.put(cs.a, pc, row.array.items[i]);
                        };
                    }
                    if (try core.planDelete(cs.a, cs.table, cs.pk, .{ .object = keyed })) |stp| {
                        _ = try cs.client.stepExec(cs.a, stp);
                    }
                    continue;
                };
                const params = try cs.a.alloc(storage.Value, row.array.items.len);
                for (row.array.items, 0..) |cell, i| params[i] = try chainCellToStorage(cs.a, cell);
                _ = st.query(cs.a, cs.sql, params) catch |err| {
                    // Read the SQLite text HERE, before the rollback clears it: it is
                    // what tells an FK refusal from a bad column apart.
                    std.debug.print("{s}: row {d} of {d} refused: {any} — sqlite: {s}\n", .{
                        cs.table, n + 1, cs.rows.items.len, err, st.errMsg(),
                    });
                    return err;
                };
            }
        }
    };

    fn applyChain(self: *SyncClient, table: []const u8) !void {
        // Per-call: a seed can be megabytes, and it is dead the moment it is applied.
        // Only two things leave this arena — the dictionary cache and `seed_stream` —
        // and both are duped into the client arena explicitly below.
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        const key = try std.fmt.allocPrint(a, "{s}.{s}", .{ self.effTenant(table), table });
        const man_bytes = (try self.t.kvGet(a, self.kv_generations, key)) orelse {
            std.debug.print("{s}: no chain yet\n", .{table});
            return;
        };
        const man = (try std.json.parseFromSlice(Value, a, man_bytes, .{})).value;

        const wm_rows = try self.st.query(a,
            "SELECT watermark FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }});
        const watermark: ?[]const u8 = if (wm_rows.len > 0 and wm_rows[0][0] == .text) wm_rows[0][0].text else null;

        const plan = try core.planFromManifest(a, man, watermark);
        // D2's destruction guard: a full from a chain older than this replica's
        // position would destroy rows CDC will never re-deliver.
        const cdc_stream = if (man.object.get("cdc_stream")) |v| (if (v == .string) v.string else "") else "";
        const pos: i64 = if (cdc_stream.len > 0) @intCast(try self.storedSeq(cdc_stream)) else 0;
        if (core.fullPredatesReplica(man, plan.array, pos)) {
            std.debug.print("{s}: chain predates replica — refusing the full (D2)\n", .{table});
            return;
        }

        const st = self.states.getPtr(table).?;
        const bucket = try std.fmt.allocPrint(a, "{s}{s}", .{ self.gen_bucket_prefix, self.effTenant(table) });
        var applied: usize = 0;
        for (plan.array.items) |step| {
            // Per-step: `raw`, `blob` and `doc` are three copies of the same seed at
            // their largest; freeing them per step bounds the peak at one step's worth.
            var sa = std.heap.ArenaAllocator.init(self.a);
            defer sa.deinit();
            const step_a = sa.allocator();
            const raw = try self.t.objectGetBytes(step_a, bucket, step.object.get("name").?.string);
            // §10x: a delta names the dictionary it was compressed with; fetch it
            // once per era from the same bucket and keep it (immutable by name, so
            // client-lifetime is the RIGHT arena here — the one place in this fn).
            var dict: ?[]const u8 = null;
            if (step.object.get("dict")) |dv| if (dv == .string) {
                if (self.dicts.get(dv.string)) |d| {
                    dict = d;
                } else {
                    const la = self.aa();
                    const d = try self.t.objectGetBytes(la, bucket, dv.string);
                    try self.dicts.put(la, try la.dupe(u8, dv.string), d);
                    dict = d;
                }
            };
            const blob = try maybeZstd(step_a, raw, dict); // §10w: magic-sniffed, mixed chains fine
            const doc = try decodeMsgpack(step_a, blob);
            const cols = try jsonStrList(step_a, doc.object.get("columns"));
            const vcol_v = doc.object.get("version_column") orelse (man.object.get("version_column") orelse @as(Value, .null));
            const vcol: ?[]const u8 = if (vcol_v == .string and contains(cols, vcol_v.string)) vcol_v.string else null;
            const rows = doc.object.get("rows").?.array;
            const cs = ChainStep{
                .client = self,
                .a = step_a,
                .table = table,
                .sql = try core.chainUpsertSql(step_a, table, cols, st.pk, vcol),
                .rows = rows,
                .is_full = std.mem.eql(u8, step.object.get("kind").?.string, "full"),
                .cols = cols,
                .pk = st.pk,
                .tomb_idx = if (st.tombstone_col) |tc| indexOf(cols, tc) else null,
            };
            self.st.transaction(cs, ChainStep.apply) catch |err| {
                // The SQLite text is the only thing that distinguishes a bad row from
                // a bad schema; a bare StepFailed here cost a run to find out which.
                std.debug.print("{s}: chain step {s} ({s}, {d} rows) rolled back: {any}\n", .{
                    table, step.object.get("name").?.string, if (cs.is_full) "full" else "delta",
                    rows.items.len, err,
                });
                return err;
            };
            applied += rows.items.len;
        }

        // Anchors (findings 7 + 10): the ONE place the gate may anchor to.
        if (man.object.get("cutoff_seq")) |v| if (v == .integer and v.integer > 0) {
            st.seed_seq = @intCast(v.integer);
            st.seed_stream = if (cdc_stream.len > 0) try self.aa().dupe(u8, cdc_stream) else null;
        };
        if (man.object.get("cutoff_lsn")) |v| if (v == .string) {
            st.seed_lsn = core.lsnToNumber(v.string);
        };
        const cv = if (man.object.get("cutoff_version")) |v| (if (v == .string) v.string else "") else "";
        // The chain's cutoff is a version this replica has now seen (every seeded row is
        // at or below it): feed the HLC floor, as the TS client does.
        if (cv.len > 0) {
            const norm = try core.normalizeVersion(a, try core.pgTsToWire(a, cv));
            const floor = core.maxVersion(self.seen_floor, norm);
            if (floor.ptr != self.seen_floor.ptr and floor.len <= self.seen_floor_buf.len) {
                @memcpy(self.seen_floor_buf[0..floor.len], floor);
                self.seen_floor = self.seen_floor_buf[0..floor.len];
            }
        }
        _ = try self.st.query(a,
            "INSERT INTO _zbz_generations (tbl, watermark, cutoff_lsn) VALUES (?, ?, ?) ON CONFLICT(tbl) DO UPDATE SET watermark = excluded.watermark, cutoff_lsn = excluded.cutoff_lsn",
            &.{ .{ .text = table }, .{ .text = cv }, .{ .integer = st.seed_lsn orelse 0 } });
        std.debug.print("{s}: seeded {d} row(s) from chain g{d}\n", .{ table, applied, if (man.object.get("gen")) |v| v.integer else 0 });
    }

    // ─── step 3: CDC catch-up (gate → apply → hold → positions) ─────────────

    pub fn drainCdc(self: *SyncClient) !void {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        var streams: std.StringArrayHashMapUnmanaged(void) = .empty;
        // Public first: parents (users) ride CDC_PUBLIC — fewer FK holds.
        var it = self.states.iterator();
        while (it.next()) |e| {
            if (std.mem.eql(u8, e.value_ptr.route, self.cdc_public)) try streams.put(a, e.value_ptr.route, {});
        }
        it = self.states.iterator();
        while (it.next()) |e| try streams.put(a, e.value_ptr.route, {});

        var sit = streams.iterator();
        while (sit.next()) |se| try self.drainStream(se.key_ptr.*);

        // FK hold/retry (§10h): one bulk retry pass after every stream drained.
        var resolved: usize = 0;
        for (self.held.items) |h| {
            if (self.applyEvent(h.table, h.ev, 0)) |_| {
                resolved += 1;
            } else |_| {}
        }
        if (self.held.items.len > 0) {
            std.debug.print("fk held: {d}, resolved on retry: {d}\n", .{ self.held.items.len, resolved });
        }
        // The pass is over: drop the list AND its events in one reset. The list's
        // backing array lives in the same arena, so it must be re-zeroed, not
        // `clearRetainingCapacity`'d — that would keep a pointer into reset memory.
        self.held = .empty;
        _ = self.held_arena.reset(.retain_capacity);
    }

    fn drainStream(self: *SyncClient, stream: []const u8) !void {
        const last = try self.storedSeq(stream);
        var name_buf: [48]u8 = undefined;
        // Unique-enough per process: pid + a monotonic counter (std.crypto.random
        // wants an Io in 0.16; libc is already linked).
        const Ctr = struct {
            var n = std.atomic.Value(u32).init(0);
        };
        const cname = try std.fmt.bufPrint(&name_buf, "zbz{d}x{d}", .{ std.c.getpid(), Ctr.n.fetchAdd(1, .monotonic) });

        var cfg = transportConsumerConfig();
        cfg.name = cname;
        cfg.durable_name = cname;
        // ⚠️ Reaped by the SERVER, not deleted by us. The consumer is named (nats.zig's
        // `pullSubscribe` requires it, and sets it durable), and the client JWT has no
        // `$JS.API.CONSUMER.DELETE` grant — by design — so deleting it from here was
        // refused and cost a 5 s timeout per stream (measured 2026-08-29). An inactive
        // threshold makes the server do it: nats-server >= 2.9 honours it on durable
        // consumers as well. 30 s is well past any fetch gap in a drain and short
        // enough that a client draining on a timer never accumulates consumers.
        // (Field added by the local nats.zig patch — nats.zig/NOTES.md.)
        cfg.inactive_threshold = 30 * std.time.ns_per_s;
        if (last > 0) {
            cfg.deliver_policy = .by_start_sequence;
            cfg.opt_start_seq = last + 1;
        } else {
            cfg.deliver_policy = .all;
        }
        const sub = try self.t.js.pullSubscribe(null, cname, .{ .stream = stream, .config = cfg });
        defer sub.deinit(); // the server reaps the consumer itself — see inactive_threshold above

        var max_seq: u64 = last;
        while (true) {
            // ⚠️ Only a TIMEOUT means "caught up". A closed connection or a slow
            // consumer used to break here too — and then persist the position, which
            // is how a network blip becomes a recorded claim to have read the tail.
            var batch = sub.fetch(100, .{ .duration = .{ .raw = .fromMilliseconds(900), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => break,
                else => return err,
            };
            defer batch.deinit();
            if (batch.messages.len == 0) break; // caught up to the tail

            // Per-batch: every decoded event dies with the batch, except the FK-held
            // ones, which are deep-copied into `held_arena` below.
            var ba = std.heap.ArenaAllocator.init(self.a);
            defer ba.deinit();
            const a = ba.allocator();
            for (batch.messages) |m| {
                const seq = m.metadata.sequence.stream;
                const doc = decodeMsgpack(a, m.msg.data) catch {
                    m.ack() catch {};
                    continue;
                };
                const events: []const Value = if (doc == .array) doc.array.items else &.{doc};
                for (events) |ev| {
                    if (ev != .object) continue;
                    const table = if (ev.object.get("table")) |v| (if (v == .string) v.string else continue) else continue;
                    if (self.states.get(table) == null) continue;
                    self.applyEvent(table, ev, seq) catch |err| switch (err) {
                        // An OOM here is a `try`, not a `catch {}`: a dropped hold is an
                        // event that is acked, positioned past, and never applied.
                        error.FkHeld => {
                            const ha = self.held_arena.allocator();
                            try self.held.append(ha, .{ .table = try ha.dupe(u8, table), .ev = try cloneValue(ha, ev) });
                        },
                        else => {},
                    };
                }
                m.ack() catch {};
                if (seq > max_seq) max_seq = seq;
            }
            // D1: delivery + accounting IS the position — applied, gated or held.
            if (max_seq > last) try self.persistSeq(stream, max_seq);
        }
        if (max_seq > last) try self.persistSeq(stream, max_seq);
        std.debug.print("{s}: drained to seq {d}\n", .{ stream, max_seq });
    }

    fn applyEvent(self: *SyncClient, table: []const u8, ev: Value, seq: u64) !void {
        const st = self.states.get(table).?;
        // The seed gate (findings 7 + 10) — seq primary, seed-anchored lsn fallback.
        if (st.seed_seq != null and st.seed_stream != null) {
            if (seq != 0 and std.mem.eql(u8, st.seed_stream.?, st.route) and seq <= st.seed_seq.?) return;
        } else if (st.seed_lsn) |floor| {
            const lsn: i64 = if (ev.object.get("lsn")) |v| (if (v == .integer) v.integer else 0) else 0;
            if (lsn != 0 and lsn < floor) return;
        }

        const op = if (ev.object.get("operation")) |v| (if (v == .string) v.string else "") else "";
        const data = ev.object.get("data") orelse return;
        if (data != .object) return;

        var ta = std.heap.ArenaAllocator.init(self.a);
        defer ta.deinit();
        const taa = ta.allocator();

        // ⚠️ The HLC floor (§7.2, NOTES §10q), fed from every arriving row's version
        // column — OBSERVED remote versions, never our own stamps. This field was
        // declared, read by `hlcVersion` and never written (found by the 2026-08-29
        // NOTES reread): a lagging clock could stamp under a row this replica had
        // already seen, the exact case the floor exists to prevent. A fixed buffer, not
        // an arena dupe, for the same reason as `last_version`.
        if (st.version_col) |vc| if (data.object.get(vc)) |sv| if (sv == .string) {
            const wire = try core.pgTsToWire(taa, sv.string);
            const norm = try core.normalizeVersion(taa, wire);
            const floor = core.maxVersion(self.seen_floor, norm);
            if (floor.ptr != self.seen_floor.ptr and floor.len <= self.seen_floor_buf.len) {
                @memcpy(self.seen_floor_buf[0..floor.len], floor);
                self.seen_floor = self.seen_floor_buf[0..floor.len];
            }
        };

        // A hard DELETE (tables without a tombstone column) and a soft delete (an
        // update that SETS the tombstone, §7.5) end the same way here: the row goes.
        if (std.mem.eql(u8, op, "DELETE") or tombstoned(st, data)) {
            if (try core.planDelete(taa, table, st.pk, data)) |stp| {
                _ = try self.stepExec(taa, stp);
            }
            return;
        }
        if (try core.planKeyChange(taa, table, st.pk, data)) |kc| _ = try self.stepExec(taa, kc);
        const up = try self.updateOrUpsert(taa, table, st, op, data);
        _ = self.stepExec(taa, up) catch |err| {
            if (err == storage.Error.StepFailed and
                std.mem.indexOf(u8, self.st.errMsg(), "FOREIGN KEY") != null)
            {
                return error.FkHeld;
            }
            return err;
        };
    }

    /// An UPDATE for a row that is HERE is applied as an UPDATE (core.planUpdate —
    /// only the columns sent), never as the upsert whose INSERT arm fails NOT NULL on
    /// a partial payload before the conflict is resolved (§7's asymmetry; measured in
    /// the soak as a bare StepFailed on a four-field UPDATE). A row that is not here,
    /// or an INSERT, takes the upsert. The probe is core.planExists.
    fn updateOrUpsert(self: *SyncClient, a: std.mem.Allocator, table: []const u8, st: TableState, op: []const u8, data: Value) !Value {
        if (std.mem.eql(u8, op, "UPDATE")) {
            if (try core.planExists(a, table, st.pk, data)) |ex| {
                const hit = self.stepExec(a, ex) catch &.{};
                if (hit.len > 0) {
                    if (try core.planUpdate(a, table, st.pk, data)) |upd| return upd;
                }
            }
        }
        return core.planUpsert(a, table, st.pk, data);
    }

    // ── the write path (PROTOCOL.md §7.1) ───────────────────────────────────

    /// The outbox: what makes this a queue rather than a log. An entry leaves only on
    /// a definitive verdict — never on a send failure, which is why a flush is safe to
    /// repeat and why the original msg_id must survive a restart.
    pub fn ensureOutbox(self: *SyncClient) !void {
        try self.st.execSimple(
            \\CREATE TABLE IF NOT EXISTS _zebridge_outbox (
            \\  msg_id     TEXT PRIMARY KEY,
            \\  subject    TEXT NOT NULL,
            \\  payload    TEXT NOT NULL,
            \\  tbl        TEXT NOT NULL,
            \\  row_id     TEXT NOT NULL,
            \\  before     TEXT,
            \\  created_at INTEGER NOT NULL,
            \\  attempts   INTEGER NOT NULL DEFAULT 0
            \\)
        );
    }

    /// The GC watermark as THIS replica sees it — the one row of
    /// `zebridge_gc_watermark`, which arrives over CDC like any other table. null
    /// whenever the answer is not known (not replicated here, no row yet, read
    /// failed), and `core.outboxWatermarkGate` treats null as "refuse nothing".
    pub fn gcWatermark(self: *SyncClient, a: std.mem.Allocator) ?[]const u8 {
        const rows = self.st.query(a, "SELECT watermark FROM zebridge_gc_watermark LIMIT 1", &.{}) catch return null;
        if (rows.len == 0 or rows[0].len == 0) return null;
        const v = rows[0][0];
        return if (v == .text and v.text.len > 0) v.text else null;
    }

    /// One edit: stamp it, apply it locally, queue it, send it.
    ///
    /// The order is the contract. The optimistic apply and the outbox row are written
    /// BEFORE the publish, so a crash between them leaves a queued write that the next
    /// flush repeats — never a write that was sent but not remembered.
    ///
    /// `result_a` owns the returned msg_id and nothing else: everything this call
    /// builds — envelope, before-image, JSON, msgpack — is per-call and freed on return.
    pub fn mutate(self: *SyncClient, result_a: std.mem.Allocator, table: []const u8, op: []const u8, key: Value, values: ?Value) ![]const u8 {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        const st = self.states.get(table) orelse return error.UnknownTable;
        try self.ensureOutbox();

        const version = try core.hlcVersion(a, try nowWireIso(a), self.last_version, self.seen_floor);
        if (version.len > self.last_version_buf.len) return error.VersionTooLong;
        @memcpy(self.last_version_buf[0..version.len], version);
        self.last_version = self.last_version_buf[0..version.len];

        var args: std.json.ObjectMap = .empty;
        try args.put(a, "principal", .{ .string = self.opts.principal });
        try args.put(a, "clientId", .{ .string = self.opts.client_id });
        try args.put(a, "table", .{ .string = table });
        try args.put(a, "op", .{ .string = op });
        try args.put(a, "version", .{ .string = version });
        try args.put(a, "key", key);
        if (values) |v| try args.put(a, "values", v);
        var pk_arr = std.json.Array.init(a);
        for (st.pk) |c| try pk_arr.append(.{ .string = c });
        try args.put(a, "pkCols", .{ .array = pk_arr });
        try args.put(a, "mutationsPrefix", .{ .string = self.subject_mutations_prefix });

        const env = try core.buildMutation(a, .{ .object = args });
        const subject = env.object.get("subject").?.string;
        const msg_id = env.object.get("msgId").?.string;
        const row_id = env.object.get("id").?.string;
        const payload = env.object.get("payload").?;

        // The before-image, for the revert a refusal or a rejection needs. Captured
        // BEFORE the optimistic apply overwrites it, obviously.
        const before = try self.beforeImage(a, table, st, key);

        try self.applyOptimistic(table, st, env.object.get("optimistic").?);

        const payload_json = try core.valueToString(a, payload);
        const before_json: storage.Value = if (before) |b| .{ .text = try core.valueToString(a, b) } else .null;
        _ = try self.st.query(a,
            \\INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, created_at, attempts)
            \\VALUES (?,?,?,?,?,?,?,0)
            \\ON CONFLICT(msg_id) DO UPDATE SET attempts = _zebridge_outbox.attempts + 1
        , &.{
            .{ .text = msg_id }, .{ .text = subject }, .{ .text = payload_json },
            .{ .text = table },  .{ .text = row_id },  before_json,
            .{ .integer = nowMillis() },
        });

        // ⚠️ Sent through flushOutbox, NOT published directly from here.
        //
        // Publishing here was a hole the first live test walked straight into: the
        // watermark gate lives in the flush, so a write's FIRST send skipped it and
        // only replays were checked. Measured — a write the gate then refused was
        // already in PostgreSQL, printed as "flushed: 0" next to a row that existed.
        //
        // One path for every send closes it. The cost is that a flush republishes any
        // stragglers too, which is what an outbox is for, and the msg_id makes it
        // idempotent.
        _ = self.flushOutbox() catch |err| {
            // Kept, not lost: the entry is durable and the next flush retries.
            std.debug.print("mutate: send failed ({any}) — queued as {s}\n", .{ err, msg_id });
        };
        return try result_a.dupe(u8, msg_id);
    }

    /// Republish everything still queued — and refuse what the GC watermark has
    /// outlived (§10at). Returns the number actually sent.
    ///
    /// A publish failure is logged and the entry stays queued; the pass continues so
    /// one bad send does not starve the rest. If NOTHING could be sent the last error
    /// is returned — "flushed: 0" next to a broker that is down should say why.
    pub fn flushOutbox(self: *SyncClient) !usize {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        try self.ensureOutbox();
        const rows = try self.st.query(a,
            "SELECT msg_id, subject, payload, tbl, row_id, before FROM _zebridge_outbox ORDER BY created_at", &.{});
        if (rows.len == 0) return 0;

        // ⚠️ Gated ONCE, before the first publish — one read of the watermark for the
        // whole pass, and an entry that must not be sent is not sent even if an
        // earlier publish throws.
        const watermark = self.gcWatermark(a);
        var entries = std.json.Array.init(a);
        for (rows) |r| {
            var e: std.json.ObjectMap = .empty;
            try e.put(a, "msgId", .{ .string = if (r[0] == .text) r[0].text else "" });
            const env = if (r[2] == .text) parseStoredJson(a, r[2].text) catch Value.null else Value.null;
            const ver = if (env == .object) env.object.get("version") else null;
            try e.put(a, "version", if (ver) |v| v else .null);
            try entries.append(.{ .object = e });
        }
        const gate = try core.outboxWatermarkGate(a, entries, watermark);

        var refused: std.StringArrayHashMapUnmanaged(void) = .empty;
        for (gate.object.get("refuse").?.array.items) |v| try refused.put(a, v.string, {});

        var sent: usize = 0;
        var failed: usize = 0;
        var last_err: ?anyerror = null;
        var collected: usize = 0;
        for (rows) |r| {
            const msg_id = if (r[0] == .text) r[0].text else continue;
            // The verdicts this client MISSED (PROTOCOL §7.4b): one per-key direct get per
            // pending entry, settled through the same handler `drainVerdicts` uses. Until
            // 2026-08-29 nothing read a stored verdict — the replay earned a fresh one,
            // at the cost of a second PostgreSQL attempt and, past the duplicate window,
            // a `stale` for a write that had been accepted.
            if (!refused.contains(msg_id)) {
                const ack_subject = try std.fmt.allocPrint(a, "{s}.{s}.{s}", .{ self.subject_mutation_ack_prefix, self.opts.principal, msg_id });
                if (self.t.lastBySubject(self.stream_mutations, ack_subject) catch null) |vm| {
                    defer vm.deinit();
                    if (try self.settleVerdict(a, msg_id, vm.data)) {
                        collected += 1;
                        continue;
                    }
                }
            }
            if (refused.contains(msg_id)) {
                // Exactly a `rejected` verdict's handling, because that is what it is:
                // this write will never be sent, so the optimistic copy is a
                // divergence. Restore and SAY SO — the user's edit is being dropped.
                try self.revertOptimistic(a, r);
                _ = try self.st.query(a, "DELETE FROM _zebridge_outbox WHERE msg_id = ?", &.{.{ .text = msg_id }});
                std.debug.print(
                    "outbox: {s}[{s}] {s} predates the GC watermark ({s}) and CANNOT be sent — " ++
                        "its tombstone was reaped, so sending it would resurrect a deleted row " ++
                        "(PROTOCOL MUST 6). Local copy reverted; this edit is lost.\n",
                    .{ if (r[3] == .text) r[3].text else "?", if (r[4] == .text) r[4].text else "?",
                       msg_id, watermark orelse "?" },
                );
                continue;
            }
            const env = if (r[2] == .text) try parseStoredJson(a, r[2].text) else Value.null;
            self.publishEnvelope(a, r[1].text, env, msg_id) catch |err| {
                failed += 1;
                last_err = err;
                std.debug.print("outbox: {s} not sent ({any}) — still queued\n", .{ msg_id, err });
                continue;
            };
            sent += 1;
        }
        if (collected > 0) std.debug.print("outbox: collected {d} verdict(s) published while this client was away — settled without replay\n", .{collected});
        if (sent == 0 and failed > 0) return last_err.?;
        return sent;
    }

    /// One verdict, live or collected: true when definitive (the outbox entry is
    /// settled one way or another), false for `failed` (kept for retry).
    fn settleVerdict(self: *SyncClient, a: std.mem.Allocator, mid: []const u8, data: []const u8) !bool {
        const v = parseStoredJson(a, data) catch return false;
        const status = if (v == .object) (if (v.object.get("status")) |x| (if (x == .string) x.string else "") else "") else "";
        if (std.mem.eql(u8, status, "failed")) return false;
        if (std.mem.eql(u8, status, "rejected") or std.mem.eql(u8, status, "row_deleted")) {
            const rows = try self.st.query(a,
                "SELECT msg_id, subject, payload, tbl, row_id, before FROM _zebridge_outbox WHERE msg_id = ?",
                &.{.{ .text = mid }});
            if (rows.len == 1) try self.revertOptimistic(a, rows[0]);
        }
        // `stale` deliberately does NOT revert: the authoritative row is already on its
        // way over CDC, and restoring a before-image could undo a newer value that has
        // since been applied. Dropping the entry is the whole correction.
        _ = try self.st.query(a, "DELETE FROM _zebridge_outbox WHERE msg_id = ?", &.{.{ .text = mid }});
        return true;
    }

    /// Pop entries whose fate is settled. `applied` is confirmation; `rejected` and
    /// `row_deleted` are refusals that also undo the optimistic copy; `failed` is
    /// retryable and deliberately left queued.
    /// Open the verdict channel. Call BEFORE the first write — see the field comment.
    ///
    /// ⚠️ Core, not JetStream, which bounds what this can promise: verdicts are also
    /// STORED in the MUTATIONS stream precisely so a client that was offline when its
    /// write was judged can collect the verdict on reconnect (§7.1). A core
    /// subscription cannot do that — it only sees what arrives while it is open. A
    /// JetStream consumer filtered to this subject is the durable form, and the
    /// follow-up; this is correct for a client that stays connected.
    pub fn subscribeVerdicts(self: *SyncClient) !void {
        if (self.verdicts != null) return;
        const subject = try std.fmt.allocPrint(self.aa(), "{s}.{s}.>", .{ self.subject_mutation_ack_prefix, self.opts.principal });
        self.verdicts = try self.t.subscribeSync(subject);
    }

    /// `wait_ms` is a BUDGET for the first verdict, not a per-message timeout. A
    /// verdict costs the bridge a PostgreSQL round trip, so a single 250 ms wait gave
    /// up before the answer existed — measured: 0 of 1 settled while the row was
    /// already in PostgreSQL. Once messages start arriving the loop drains them all
    /// and stops at the first gap.
    pub fn drainVerdicts(self: *SyncClient, wait_ms: u64) !usize {
        try self.subscribeVerdicts();
        const sub = self.verdicts.?;
        var settled: usize = 0;
        const started = nowMillis();
        while (true) {
            if (settled == 0 and @as(u64, @intCast(nowMillis() - started)) > wait_ms) break;
            // The bridge's own idiom (nats_publisher.zig): a bounded wait, so a
            // drain with nothing pending returns instead of blocking forever.
            // Only a timeout ends the drain quietly; a closed connection is an error.
            const msg = sub.nextMsgTimeout(
                .{ .duration = .{ .raw = .fromMilliseconds(250), .clock = .awake } },
            ) catch |err| switch (err) {
                error.Timeout => break,
                else => return err,
            };
            // ⚠️ The message is OURS: `nextMsgTimeout` hands over a `*Message` and
            // nats.zig frees nothing on our behalf. Without this, every verdict leaked
            // its arena (or its pool slot) — invisible to `std.testing.allocator`
            // because no unit test drains a verdict, and to the soak because nothing
            // measured RSS across it.
            defer msg.deinit();
            // Per-verdict arena: the parsed JSON and the outbox row are dead once the
            // entry is popped.
            var va = std.heap.ArenaAllocator.init(self.a);
            defer va.deinit();
            const a = va.allocator();
            // ⚠️ The msg id is in the SUBJECT, not the body — `mutation_ack.<principal>.<msg_id>`
            // — for the same reason a mutation carries no `table` field: NATS authorizes
            // subjects and not payloads, so identity belongs where the broker can see it.
            // Reading it from the body found nothing and silently settled zero verdicts
            // while the row was already in PostgreSQL (measured).
            const mid = blk: {
                var last: []const u8 = "";
                var it = std.mem.splitScalar(u8, msg.subject, '.');
                while (it.next()) |part| last = part;
                break :blk last;
            };
            if (mid.len == 0) continue;

            // ⚠️ JSON, not msgpack. CDC and mutations are msgpack; a verdict is JSON —
            // it is read by humans and by `nats sub` as often as by a client. The wire
            // names are the bridge's: accepted / stale / row_deleted / rejected settle
            // the entry (the last two revert the optimistic copy); `failed` stays queued.
            if (try self.settleVerdict(a, mid, msg.data)) settled += 1;
        }
        return settled;
    }

    /// The row as it stands, as a JSON object — or null when there is none.
    fn beforeImage(self: *SyncClient, a: std.mem.Allocator, table: []const u8, st: TableState, key: Value) !?Value {
        var where: std.ArrayList(u8) = .empty;
        var params = try a.alloc(storage.Value, st.pk.len);
        for (st.pk, 0..) |c, i| {
            if (i > 0) try where.appendSlice(a, " AND ");
            // ⚠️ No `.writer(a)` on an unmanaged ArrayList in 0.16 — allocPrint then
            // append, which is what the rest of this file does.
            try where.appendSlice(a, try std.fmt.allocPrint(a, "\"{s}\" = ?", .{c}));
            const kv = if (key == .object) key.object.get(c) orelse Value.null else Value.null;
            params[i] = try jsonToStorage(a, kv);
        }
        const sql = try std.fmt.allocPrint(a, "SELECT * FROM \"{s}\" WHERE {s}", .{ table, where.items });
        const rows = self.st.query(a, sql, params) catch return null;
        if (rows.len == 0) return null;
        var obj: std.json.ObjectMap = .empty;
        for (st.cols, 0..) |c, i| {
            if (i >= rows[0].len) break;
            try obj.put(a, c, storageToJson(rows[0][i]));
        }
        return .{ .object = obj };
    }

    /// Apply this client's own edit locally, before the server has seen it.
    ///
    /// ⚠️ A partial payload fails here, and the message must say so. The local apply is
    /// an UPSERT, so SQLite evaluates the INSERT arm first — a payload missing a
    /// `NOT NULL` column violates it before the conflict is ever resolved, and the
    /// error is a bare `StepFailed` unless the SQLite text is carried out with it.
    /// This is PROTOCOL §7's asymmetry exactly: a partial payload succeeds on the
    /// update path and fails on the insert path, which is why the wire carries FULL
    /// rows.
    fn applyOptimistic(self: *SyncClient, table: []const u8, st: TableState, ev: Value) !void {
        var ta = std.heap.ArenaAllocator.init(self.a);
        defer ta.deinit();
        const taa = ta.allocator();
        const op = if (ev.object.get("operation")) |v| (if (v == .string) v.string else "") else "";
        const data = ev.object.get("data") orelse return;
        // The same §7.5 rule as the CDC path: our own edit that sets the tombstone
        // removes the row locally, exactly as the server's echo of it would.
        if (std.mem.eql(u8, op, "DELETE") or tombstoned(st, data)) {
            if (try core.planDelete(taa, table, st.pk, data)) |stp| _ = try self.stepExec(taa, stp);
            return;
        }
        const up = try self.updateOrUpsert(taa, table, st, op, data);
        _ = self.stepExec(taa, up) catch |err| {
            std.debug.print(
                "optimistic apply of {s} failed: {any} — sqlite: {s}\n",
                .{ table, err, self.st.errMsg() },
            );
            return err;
        };
    }

    /// Undo an optimistic apply from the stored before-image: restore it if there was
    /// a row, delete ours if there was not.
    fn revertOptimistic(self: *SyncClient, a: std.mem.Allocator, r: storage.Row) !void {
        const table = if (r[3] == .text) r[3].text else return;
        const st = self.states.get(table) orelse return;
        const env = if (r[2] == .text) try parseStoredJson(a, r[2].text) else return;
        const key = if (env == .object) env.object.get("key") orelse return else return;

        if (r[5] == .text) {
            const before = try parseStoredJson(a, r[5].text);
            const up = try core.planUpsert(a, table, st.pk, before);
            _ = self.stepExec(a, up) catch {};
        } else if (try core.planDelete(a, table, st.pk, key)) |stp| {
            _ = self.stepExec(a, stp) catch {};
        }
    }

    fn publishEnvelope(self: *SyncClient, a: std.mem.Allocator, subject: []const u8, payload: Value, msg_id: []const u8) !void {
        const bytes = try encodeMsgpack(a, payload);
        try self.t.publish(subject, bytes, msg_id);
    }

    fn stepExec(self: *SyncClient, a: std.mem.Allocator, stp: Value) ![]storage.Row {
        const params_v = stp.object.get("params").?.array;
        const params = try a.alloc(storage.Value, params_v.items.len);
        for (params_v.items, 0..) |v, i| params[i] = try jsonToStorage(a, v);
        return self.st.query(a, stp.object.get("sql").?.string, params);
    }

    pub fn count(self: *SyncClient, table: []const u8) i64 {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        const sql = std.fmt.allocPrint(qa.allocator(), "SELECT count(*) FROM {s}", .{table}) catch return -1;
        const rows = self.st.query(qa.allocator(), sql, &.{}) catch return -1;
        return rows[0][0].integer;
    }
};

/// One required string from grammar.json, by path. The error names the key, because
/// "which name did the file forget" is the whole diagnostic.
fn grammarString(root: Value, path: []const []const u8) ![]const u8 {
    var cur = root;
    for (path) |key| {
        if (cur != .object) return grammarMissing(path);
        cur = cur.object.get(key) orelse return grammarMissing(path);
    }
    if (cur != .string or cur.string.len == 0) return grammarMissing(path);
    return cur.string;
}

fn grammarMissing(path: []const []const u8) error{GrammarKeyMissing} {
    std.debug.print("grammar.json: required key missing or not a string: ", .{});
    for (path, 0..) |k, i| std.debug.print("{s}{s}", .{ if (i > 0) "." else "", k });
    std.debug.print("\n", .{});
    return error.GrammarKeyMissing;
}

test "grammar: a missing key fails naming its path, never a silent default" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ok = try std.json.parseFromSlice(Value, a, \\{"kv":{"schemas":"schemas"}}
    , .{});
    try std.testing.expectEqualStrings("schemas", try grammarString(ok.value, &.{ "kv", "schemas" }));
    try std.testing.expectError(error.GrammarKeyMissing, grammarString(ok.value, &.{ "kv", "tenants" }));
    try std.testing.expectError(error.GrammarKeyMissing, grammarString(ok.value, &.{"open_tenant"}));
    const empty = try std.json.parseFromSlice(Value, a, \\{"open_tenant":""}
    , .{});
    try std.testing.expectError(error.GrammarKeyMissing, grammarString(empty.value, &.{"open_tenant"}));
}

// ─── conversions ────────────────────────────────────────────────────────────

fn transportConsumerConfig() @import("nats").ConsumerConfig {
    return .{ .ack_policy = .explicit, .max_ack_pending = 512 };
}

fn contains(list: []const []const u8, s: []const u8) bool {
    for (list) |x| if (std.mem.eql(u8, x, s)) return true;
    return false;
}

fn indexOf(list: []const []const u8, s: []const u8) ?usize {
    for (list, 0..) |x, i| if (std.mem.eql(u8, x, s)) return i;
    return null;
}

/// PROTOCOL §7.5 — the decision is core.tombstoned, fixture-pinned in both cores.
fn tombstoned(st: TableState, data: Value) bool {
    return core.tombstoned(st.tombstone_col, data);
}

fn jsonStrList(a: std.mem.Allocator, v: ?Value) ![]const []const u8 {
    const val = v orelse return &.{};
    if (val != .array) return &.{};
    var out = try a.alloc([]const u8, val.array.items.len);
    for (val.array.items, 0..) |x, i| out[i] = if (x == .string) x.string else "";
    return out;
}

/// Chain objects may be zstd frames (§10w) — sniffed by the standard 4-byte
/// magic; decompression is pure std (std.compress.zstd), no C on the client.
fn maybeZstd(a: std.mem.Allocator, b: []const u8, dict: ?[]const u8) ![]const u8 {
    if (b.len < 4 or b[0] != 0x28 or b[1] != 0xb5 or b[2] != 0x2f or b[3] != 0xfd) return b;
    if (dict) |d| {
        // Dictionary frame: libzstd (std.compress.zstd cannot take a dictionary).
        // ZSTD_CONTENTSIZE_UNKNOWN/ERROR are (0ULL-1)/(0ULL-2): translate-c
        // overflows on the macros, so spell them out.
        const unknown: c_ulonglong = std.math.maxInt(c_ulonglong);
        const size = C.ZSTD_getFrameContentSize(b.ptr, b.len);
        if (size == unknown or size == unknown - 1) return error.ZstdSizeUnknown;
        const out = try a.alloc(u8, @intCast(size));
        const dctx = C.ZSTD_createDCtx() orelse return error.ZstdDecompressFailed;
        defer _ = C.ZSTD_freeDCtx(dctx);
        const n = C.ZSTD_decompress_usingDict(dctx, out.ptr, out.len, b.ptr, b.len, d.ptr, d.len);
        if (C.ZSTD_isError(n) != 0) return error.ZstdDecompressFailed;
        return out[0..n];
    }
    var out: std.Io.Writer.Allocating = .init(a);
    defer out.deinit();
    var in: std.Io.Reader = .fixed(b);
    var zs: std.compress.zstd.Decompress = .init(&in, &.{}, .{});
    _ = try zs.reader.streamRemaining(&out.writer);
    return try out.toOwnedSlice();
}

/// Stored JSON (an outbox payload or before-image) → the core's Value.
///
/// `.value` is arena-owned by the caller's allocator here, which is the client arena —
/// the same lifetime rule the rest of this file uses for parsed grammar and schemas.
fn parseStoredJson(a: std.mem.Allocator, text: []const u8) !Value {
    const parsed = try std.json.parseFromSlice(Value, a, text, .{});
    return parsed.value;
}

/// std.json.Value → msgpack bytes: the inverse of decodeMsgpack, for the write path.
///
/// The wire is msgpack in both directions (§7 — a `table` field in the body would be
/// worth nothing, so the envelope carries only key/version/data and the SUBJECT
/// carries identity, table and operation).
/// ⚠️ The error set is spelled out: Zig cannot INFER one for a recursive function,
/// and `mapPut`/`setArrElement` add NotMap/NotArray to OutOfMemory.
fn jsonToMsgpack(a: std.mem.Allocator, v: Value) error{ OutOfMemory, NotMap, NotArray }!msgpack.Payload {
    return switch (v) {
        .null => .{ .nil = {} },
        .bool => |b| .{ .bool = b },
        .integer => |i| .{ .int = i },
        .float => |f| .{ .float = f },
        .number_string => |x| try msgpack.Payload.strToPayload(x, a),
        .string => |x| try msgpack.Payload.strToPayload(x, a),
        .array => |arr| blk: {
            var pl = try msgpack.Payload.arrPayload(arr.items.len, a);
            for (arr.items, 0..) |item, i| try pl.setArrElement(i, try jsonToMsgpack(a, item));
            break :blk pl;
        },
        .object => |obj| blk: {
            var pl = msgpack.Payload.mapPayload(a);
            var it = obj.iterator();
            while (it.next()) |e| try pl.mapPut(e.key_ptr.*, try jsonToMsgpack(a, e.value_ptr.*));
            break :blk pl;
        },
    };
}

/// ⚠️ A growing writer, not a fixed 1 MiB buffer. The fixed buffer was allocated per
/// publish from whatever arena the caller passed — and the caller passed the
/// client-lifetime arena, so every write pinned a megabyte until `deinit`. Virtual
/// and untouched, so RSS never showed it; only VSZ would have.
fn encodeMsgpack(a: std.mem.Allocator, v: Value) ![]const u8 {
    const payload = try jsonToMsgpack(a, v);
    var reader = std.Io.Reader.fixed(&[_]u8{});
    var out: std.Io.Writer.Allocating = .init(a);
    defer out.deinit();
    var packer = msgpack.PackerIO.init(&reader, &out.writer);
    try packer.write(payload);
    return try out.toOwnedSlice();
}

/// Deep copy of a Value into `a` — for the one case where a Value must outlive the
/// arena it was decoded into (an FK-held event, see `held_arena`).
/// ⚠️ The error set is explicit because the function is recursive (as `jsonToMsgpack`).
fn cloneValue(a: std.mem.Allocator, v: Value) error{OutOfMemory}!Value {
    return switch (v) {
        .null, .bool, .integer, .float => v,
        .number_string => |s| .{ .number_string = try a.dupe(u8, s) },
        .string => |s| .{ .string = try a.dupe(u8, s) },
        .array => |arr| blk: {
            var out = try std.json.Array.initCapacity(a, arr.items.len);
            for (arr.items) |item| out.appendAssumeCapacity(try cloneValue(a, item));
            break :blk .{ .array = out };
        },
        .object => |obj| blk: {
            var out: std.json.ObjectMap = .empty;
            try out.ensureTotalCapacity(a, obj.count());
            var it = obj.iterator();
            while (it.next()) |e| {
                out.putAssumeCapacity(try a.dupe(u8, e.key_ptr.*), try cloneValue(a, e.value_ptr.*));
            }
            break :blk .{ .object = out };
        },
    };
}

/// A stored cell → the JSON the core speaks (for the before-image).
fn storageToJson(v: storage.Value) Value {
    return switch (v) {
        .null => .null,
        .integer => |i| .{ .integer = i },
        .real => |f| .{ .float = f },
        .text => |t| .{ .string = t },
        .blob => |b| .{ .string = b },
        .boolean => |b| .{ .bool = b },
    };
}

/// ⚠️ Zig 0.16's `std.time` has NO timestamp functions — no `nanoTimestamp`, no
/// `milliTimestamp`. libc is linked here (SQLite needs it), so `clock_gettime` is the
/// one that exists, and REALTIME is required rather than MONOTONIC: this is a wall
/// clock stamp that another machine will compare against, not an interval.
fn nowRealtime() std.c.timespec {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return ts;
}

fn nowMillis() i64 {
    const ts = nowRealtime();
    return @as(i64, ts.sec) * 1000 + @divTrunc(@as(i64, ts.nsec), 1_000_000);
}

/// Wall clock in the §7.2 wire format: UTC, six fractional digits, trailing Z.
fn nowWireIso(a: std.mem.Allocator) ![]const u8 {
    const ts = nowRealtime();
    const secs: u64 = @intCast(ts.sec);
    const micros: u64 = @intCast(@divTrunc(@as(i64, ts.nsec), 1000));
    const es = std.time.epoch.EpochSeconds{ .secs = secs };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    return std.fmt.allocPrint(a, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}Z", .{
        yd.year, md.month.numeric(), @as(u32, md.day_index) + 1,
        ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(), micros,
    });
}

/// ⚠️ A decoder with the container caps LIFTED. zig_msgpack's `PackerIO` refuses any
/// array or map over 1,000,000 entries (`ParseLimits`) — a defence against hostile
/// input. A chain full is one `rows` array holding the whole table, and `users` is
/// two million rows: the stock decoder answered `ArrayTooLarge` and seeding died on
/// the first object (measured 2026-08-29 — the soak never seeds, so nothing noticed).
/// Everything decoded here arrives on subjects the broker authorised, from the
/// bridge; the byte limits and the depth cap stay at their defaults.
const WideReaderCtx = struct {
    reader: *std.Io.Reader,
    fn read(self: WideReaderCtx, buf: []u8) std.Io.Reader.Error!usize {
        try self.reader.readSliceAll(buf);
        return buf.len;
    }
};
const WideWriterCtx = struct {
    writer: *std.Io.Writer,
    fn write(self: WideWriterCtx, bytes: []const u8) std.Io.Writer.Error!usize {
        try self.writer.writeAll(bytes);
        return bytes.len;
    }
};
const WidePack = msgpack.PackWithLimits(
    WideWriterCtx,
    WideReaderCtx,
    std.Io.Writer.Error,
    std.Io.Reader.Error,
    WideWriterCtx.write,
    WideReaderCtx.read,
    .{ .max_array_length = 1 << 31, .max_map_size = 1 << 31 },
);

/// msgpack bytes → std.json.Value (the shape core.zig speaks).
fn decodeMsgpack(a: std.mem.Allocator, bytes: []const u8) !Value {
    var reader = std.Io.Reader.fixed(bytes);
    var dummy: [0]u8 = .{};
    var writer = std.Io.Writer.fixed(&dummy);
    var packer = WidePack.init(.{ .writer = &writer }, .{ .reader = &reader });
    const payload = try packer.read(a);
    return mpToJson(a, payload);
}

fn decodeMaybeMsgpackString(a: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    const v = decodeMsgpack(a, bytes) catch return bytes;
    return if (v == .string) v.string else bytes;
}

fn mpToJson(a: std.mem.Allocator, p: msgpack.Payload) error{OutOfMemory}!Value {
    return switch (p) {
        .nil => .null,
        .bool => |b| .{ .bool = b },
        .int => |i| .{ .integer = i },
        .uint => |u| if (u <= std.math.maxInt(i64)) Value{ .integer = @intCast(u) } else Value{ .float = @floatFromInt(u) },
        .float => |f| .{ .float = f },
        .str => |s| .{ .string = s.value() },
        .bin => |b| .{ .string = b.value() },
        .arr => |items| blk: {
            var arr = std.json.Array.init(a);
            for (items) |item| try arr.append(try mpToJson(a, item));
            break :blk .{ .array = arr };
        },
        .map => |m| blk: {
            var obj: std.json.ObjectMap = .empty;
            var it = m.map.iterator();
            while (it.next()) |e| {
                const k = e.key_ptr.*;
                if (k != .str) continue;
                try obj.put(a, k.str.value(), try mpToJson(a, e.value_ptr.*));
            }
            break :blk .{ .object = obj };
        },
        else => .null,
    };
}

/// A core-built SQL param (json.Value) → a storage bind value.
fn jsonToStorage(a: std.mem.Allocator, v: Value) !storage.Value {
    return switch (v) {
        .null => .null,
        .bool => |b| .{ .boolean = b },
        .integer => |i| .{ .integer = i },
        .float => |f| .{ .real = f },
        .string => |s| .{ .text = s },
        else => .{ .text = try core.valueToString(a, v) },
    };
}

/// A chain-row cell (json.Value from msgpack) → a storage bind value,
/// mirroring core.chainRowParams: structured → JSON text, strings → wire ts.
fn chainCellToStorage(a: std.mem.Allocator, v: Value) !storage.Value {
    return switch (v) {
        .string => |s| .{ .text = try core.pgTsToWire(a, s) },
        .object, .array => .{ .text = try core.valueToString(a, v) },
        else => jsonToStorage(a, v),
    };
}

// ─── ownership tests ────────────────────────────────────────────────────────
//
// `std.testing.allocator` fails a test that leaks, which is what makes these
// meaningful: they assert that a FAILED init frees everything it acquired. Before the
// errdefer chain was made exact, the first of these leaked the struct and the second
// leaked an open SQLite handle — neither showed up as a test failure anywhere, because
// nothing was calling init on a path that fails.

test "cloneValue: a held event survives the arena it was decoded into" {
    var keep = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer keep.deinit();
    var copy: Value = undefined;
    {
        var batch = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer batch.deinit();
        const src = try std.json.parseFromSlice(Value, batch.allocator(),
            \\{"table":"salaries","data":{"id":7,"tags":["a","b"],"amt":1.5,"gone":null}}
        , .{});
        copy = try cloneValue(keep.allocator(), src.value);
    } // the batch arena is gone; every byte the copy points at must be in `keep`
    try std.testing.expectEqualStrings("salaries", copy.object.get("table").?.string);
    const data = copy.object.get("data").?.object;
    try std.testing.expectEqual(@as(i64, 7), data.get("id").?.integer);
    try std.testing.expectEqualStrings("b", data.get("tags").?.array.items[1].string);
    try std.testing.expect(data.get("gone").? == .null);
}

test "msgpack encode/decode roundtrip allocates only what it writes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const src = try std.json.parseFromSlice(Value, a, \\{"key":{"uid":"u1"},"version":"2026-01-01T00:00:00.000000Z","n":[1,2,3]}
    , .{});
    const bytes = try encodeMsgpack(a, src.value);
    // The old fixed buffer was 1 MiB per call; the envelope is well under 100 bytes.
    try std.testing.expect(bytes.len < 128);
    const back = try decodeMsgpack(a, bytes);
    try std.testing.expectEqualStrings("u1", back.object.get("key").?.object.get("uid").?.string);
    try std.testing.expectEqual(@as(i64, 3), back.object.get("n").?.array.items[2].integer);
}

test "init that cannot open the database leaks nothing" {
    const bad = "/nonexistent-directory-for-zb-tests/replica.sqlite3";
    const r = SyncClient.init(std.testing.allocator, .{
        .url = "nats://127.0.0.1:1",
        .creds_path = "/nonexistent.creds",
        .grammar_path = "../grammar.json",
        .db_path = bad,
        .principal = "t",
        .tables = &.{},
    });
    try std.testing.expectError(storage.Error.OpenFailed, r);
}

test "init that cannot read the grammar leaks nothing — including the open database" {
    // The database DOES open here, so this is the case that used to leave a live
    // SQLite handle behind: errdefer destroyed the struct and closed nothing.
    const r = SyncClient.init(std.testing.allocator, .{
        .url = "nats://127.0.0.1:1",
        .creds_path = "/nonexistent.creds",
        .grammar_path = "/nonexistent-grammar.json",
        .db_path = "zbz-test-ownership.sqlite3",
        .principal = "t",
        .tables = &.{},
    });
    try std.testing.expect(std.meta.isError(r));
    _ = std.c.unlink("zbz-test-ownership.sqlite3");
}

