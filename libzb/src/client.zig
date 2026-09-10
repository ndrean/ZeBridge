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

/// §10dq: the wire grammar, compiled in — the same `src/grammar.json` the bridge
/// embeds (§10ci). A rename is a protocol fork whose cost is a rebuild, on both
/// sides; nothing reads a file, nothing fetches, and a client opens with the bridge
/// down (NATS holds everything a provisioned client needs).
pub const grammar_json: []const u8 = @embedFile("grammar");

/// sha256 of the embedded grammar, lowercase hex — what the bridge's `X-Grammar-Hash`
/// header and the /enroll payload's `grammar_hash` carry for the same bytes.
pub fn grammarHashHex(buf: *[64]u8) []const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(grammar_json, &digest, .{});
    return std.fmt.bufPrint(buf, "{x}", .{&digest}) catch unreachable;
}

pub const Options = struct {
    url: []const u8,
    creds_path: []const u8,
    /// The grammar hash the host RECEIVED — from the /enroll payload beside the JWT, or
    /// GET /grammar's header. When set, a mismatch refuses to open: this library is
    /// built for another protocol than the bridge it is pointed at. Unset skips the
    /// check (a bridge that could not be reached is not a mismatch).
    grammar_hash: ?[]const u8 = null,
    db_path: [*:0]const u8,
    principal: []const u8,
    /// Parents FIRST is still the recommended order — but since §10cp the seed
    /// runs with foreign_keys OFF (chains are per-table snapshots cut at different
    /// moments, so no order can fully protect a child chain), re-enables them
    /// after, and reports surviving violations loudly.
    tables: []const []const u8,
    /// This replica's identity, and it must be STABLE across restarts: it is the
    /// tiebreak value the bridge stores and the prefix of every msg_id, so a client
    /// that changes it loses idempotency on anything still unconfirmed.
    client_id: []const u8 = "zig-client",
    /// Fleet heartbeat cadence (NOTES §10dc); 0 disables. The bridge's bucket TTL
    /// defaults to three of these.
    heartbeat_ms: u64 = 30_000,
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
    /// For a tenant-scoped table: the stream its OPEN-TENANT rows ride (CDC_PUBLIC).
    /// Those are the shared rows every tenant may read — `zb_reader_all` admits
    /// `tenant_col = <open tenant>`, and the chain carries them because the producer
    /// reads under that policy — so the table depends on TWO streams, and a gap on
    /// either leaves half of it stale (NOTES §10bq). Null for a public table, whose
    /// only route already IS that stream.
    shared_route: ?[]const u8 = null,
    seed_seq: ?u64 = null,
    seed_stream: ?[]const u8 = null,
    seed_lsn: ?i64 = null,
    /// The catalogue's seed_epoch the descriptor carried (§10df).
    seed_epoch: i64 = 0,
};

pub const SyncClient = struct {
    a: std.mem.Allocator,
    arena: std.heap.ArenaAllocator, // client-lifetime allocations (states, grammar)
    t: *transport.Transport,
    st: storage.Storage,
    /// The app's connection: the same file, opened read-only (NOTES §10). `query` runs
    /// here and nowhere else, so the replica cannot be written around `mutate`.
    ro: storage.Storage,
    opts: Options,
    /// `syncOnce` runs the one-time steps once; the per-pass steps every call.
    schemas_synced: bool = false,

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
    kv_live: []const u8 = "live", // kv.live — optional in the grammar (a bridge older than §10dc has no key)
    last_heartbeat_ms: i64 = 0,
    kv_generations: []const u8 = undefined, // generations.kv
    gen_bucket_prefix: []const u8 = undefined, // generations.bucket_prefix
    open_tenant: []const u8 = undefined, // open_tenant
    /// §10dl: no `$KV.tenants.<principal>` key — the principal was never mapped, or was
    /// revoked. Tenant-scoped tables are then unreadable (no stream to grant) and are
    /// SKIPPED, audibly, the TS client's rule; public tables still follow.
    tenant_missing: bool = false,
    /// §10dm: the ban was seen (`mutation_ack.<principal>.revoked`): the connection is
    /// closed and every later call answers `error.Revoked`. The rows stay; the wipe is
    /// the application's explicit act (`zb_client_wipe`).
    revoked: bool = false,
    hung_up: bool = false,
    stream_mutations: []const u8 = undefined, // streams.mutations
    subject_mutations_prefix: []const u8 = undefined, // subjects.mutations_prefix
    subject_mutation_ack_prefix: []const u8 = undefined, // subjects.mutation_ack_prefix

    tenant: []const u8 = "",
    /// §10x dictionaries by object name — immutable, so the cache cannot go stale.
    dicts: std.StringArrayHashMapUnmanaged([]const u8) = .empty,
    states: std.StringArrayHashMapUnmanaged(TableState) = .empty,
    /// §10do: UPDATEs judged `stale` whose columns may still be rebased onto the
    /// winning row, keyed by msg_id; strings live in the client arena (rare, small).
    rebase: std.StringArrayHashMapUnmanaged(Rebase) = .empty,
    rebase_due: bool = false,
    /// Cumulative verdict counts by status, for the host (the flush report carries
    /// them): a single-writer run with anything but `accepted` here has a finding.
    verdict_counts: VerdictCounts = .{},
    /// FK-held events (§10h) outlive the `drainStream` call that decoded them — they
    /// wait for `drainCdc`'s retry pass — but not the pass itself. So they get their
    /// own arena, reset after every pass: a third lifetime, between "this call" and
    /// "this client", and the only one that needs a deep copy (see `cloneValue`).
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
    /// Live schema (CLIENTS.md divergence 2, ported from the TS `watchSchemas`): a KV
    /// watch on the schemas bucket, drained at the top of every poll, so a host that
    /// only polls still follows a migration. Opened lazily on the first poll.
    schema_kv: ?@import("nats").KV = null,
    /// §10df: a descriptor arrived with a higher seed_epoch — seed on the next poll.
    reseed_pending: bool = false,
    schema_watch: ?@import("nats").KVWatcher = null,
    last_version: []const u8 = "",
    last_version_buf: [64]u8 = undefined,
    /// Held open across mutate/flush: a CORE subscription only delivers what arrives
    /// while it exists, so subscribing after publishing misses the verdict every time
    /// (measured — the demo settled 0 of 1 until this was split out of drainVerdicts).
    verdicts: ?*@import("nats").Subscription = null,
    /// Live tailing (§10bh): one persistent pull consumer per CDC stream, created by
    /// the first `poll` and kept across calls. `stream` points into `states`' routes
    /// (client-lifetime memory).
    tails: std.ArrayListUnmanaged(Tail) = .empty,
    /// The ONE inbox every tail answers into (nats.zig `PullInbox`): `poll` is a single
    /// wait over all streams, ended by the first message from any of them.
    tail_inbox: ?*@import("nats").PullInbox = null,

    const Tail = struct {
        stream: []const u8,
        sub: *@import("nats").PullSubscription,
    };
    /// The server reaps a tail consumer idle this long (`inactive_threshold`). A host
    /// that stops polling for longer gets a fresh consumer at the stored position —
    /// the reaped one answers the next fetch with `NoResponders` (nats.zig patch).
    const tail_inactive_ns: u64 = 120 * std.time.ns_per_s;

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
            .t = undefined,
            .st = undefined,
            .ro = undefined,
            .opts = opts,
        };
        errdefer self.arena.deinit();

        self.st = try storage.Storage.open(opts.db_path);
        errdefer self.st.close();
        // After the read-write open: that is what creates the file.
        self.ro = try storage.Storage.openReadOnly(opts.db_path);
        errdefer self.ro.close();

        // Before the transport: the grammar is compiled in (§10dq), and a hash
        // mismatch must refuse before a socket is opened with the wrong names.
        try self.loadGrammar();

        self.t = try transport.Transport.connect(a, .{ .url = opts.url, .creds_path = opts.creds_path });
        errdefer self.t.deinit();

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
        self.ro.close();
        self.st.close();
        self.arena.deinit();
        self.a.destroy(self);
    }

    /// Everything acquired after the transport, released before it — in ONE place, so
    /// that adding a handle has an obvious home and cannot be forgotten the way
    /// `verdicts` was. (Only `deinit` calls it today; a reconnect that had to release
    /// and re-acquire this set would call the same function.)
    fn releaseHandles(self: *SyncClient) void {
        if (self.schema_watch) |*w| {
            w.deinit();
            self.schema_watch = null;
        }
        if (self.schema_kv) |*kv| {
            kv.deinit();
            self.schema_kv = null;
        }
        if (self.verdicts) |sub| {
            sub.deinit();
            self.verdicts = null;
        }
        self.dropTails();
        self.t.deinit();
    }

    fn dropTails(self: *SyncClient) void {
        for (self.tails.items) |t| t.sub.deinit();
        self.tails.clearRetainingCapacity();
        // After the subscriptions that borrow it.
        if (self.tail_inbox) |ib| {
            ib.deinit();
            self.tail_inbox = null;
        }
    }

    fn aa(self: *SyncClient) std.mem.Allocator {
        return self.arena.allocator();
    }

    fn loadGrammar(self: *SyncClient) !void {
        // Client-lifetime arena, deliberately: the grammar strings below are read for
        // the life of the client, and this runs once.
        const a = self.aa();
        if (self.opts.grammar_hash) |want| {
            var buf: [64]u8 = undefined;
            const have = grammarHashHex(&buf);
            if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, want, " \t\r\n"), have)) {
                std.debug.print("grammar mismatch: this library embeds {s}, the bridge serves {s} — built for another protocol; rebuild the client\n", .{ have, want });
                return error.GrammarMismatch;
            }
        }
        const g = try std.json.parseFromSlice(Value, a, grammar_json, .{});
        const root = g.value;
        self.cdc_prefix = try grammarString(root, &.{ "cdc_streams", "tenant_prefix" });
        self.cdc_public = try grammarString(root, &.{ "cdc_streams", "public" });
        self.kv_schemas = try grammarString(root, &.{ "kv", "schemas" });
        self.kv_tenants = try grammarString(root, &.{ "kv", "tenants" });
        self.kv_live = grammarString(root, &.{ "kv", "live" }) catch "live";
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
            self.tenant_missing = true;
            std.debug.print("tenant: {s} has no mapping ($KV.tenants.{s}) — revoked, or never enrolled: tenant-scoped tables are skipped, public tables follow\n", .{ self.opts.principal, self.opts.principal });
            return;
        };
        self.tenant_missing = false;
        // The value may be msgpack-encoded (the bridge's KV diversion) or raw.
        self.tenant = decodeMaybeMsgpackString(self.aa(), bytes) catch bytes;
        std.debug.print("tenant: {s} -> {s}\n", .{ self.opts.principal, self.tenant });
    }

    // ─── step 1: schemas → local DDL, existence from the DATABASE (finding 9) ─

    /// The outcome of one table's migration — what `syncSchemas` logs and what the
    /// unit test asserts.
    pub const Migration = enum { created, unchanged, altered, rebuilt, rekeyed, emptied };

    /// Bring one replica table in line with its published descriptor, deciding from
    /// the DATABASE (finding 9: `PRAGMA table_info`, `sqlite_master`), never from
    /// memory. The port of the TS shell's `applySchema` decision path, with the
    /// planner already in `core` (fixture-pinned): first sight → create; column
    /// diff (rename-aware) → ALTER; an FK change, or an ALTER SQLite refuses →
    /// rebuild copying the common columns; the `_view` dropped before and recreated
    /// after; indexes synced on EVERY path, because an index added upstream changes
    /// no column and lands on the "unchanged" path (the TS shell's ⚠️).
    ///
    /// Touches only `st`, so it is testable against a scratch SQLite with no NATS.
    /// (§10bi — the "increment 2" this file promised in §10v.)
    pub fn migrateTable(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, val: Value) !Migration {
        // Schema surgery ahead (possibly): cached statements referencing this
        // table would fail forever after a rebuild — clear first, cheap certainty.
        st.clearStmtCache();
        // ⚠️ Never `.?` into a descriptor. A suspension (`{"table":…,"suspended":…}`,
        // §5) has no columns, and a host behind the C ABI cannot catch a Zig panic —
        // measured 2026-08-29: a refused table's 97-byte suspension reached this
        // function and took a Python process down. Unusable → an error the caller
        // logs and skips; the replica keeps what it has.
        const cols_v = descriptorColumns(val) orelse return error.SchemaUnusable;
        const pk = try jsonStrList(a, val.object.get("pk_columns"));
        var names: std.ArrayList([]const u8) = .empty;
        for (cols_v.array.items) |c| try names.append(a, c.object.get("name").?.string);
        const empty_arr = Value{ .array = std.json.Array.init(a) };
        const fks = val.object.get("foreign_keys") orelse empty_arr;
        const idx = val.object.get("indexes") orelse empty_arr;
        const renamed = val.object.get("renamed") orelse Value{ .object = .empty };

        // §10dj: a host killed between a rebuild's DROP and its RENAME leaves the rows
        // in `<table>__migrating` and no `<table>`: finish the rename, the rows are there.
        {
            const tmp = try std.fmt.allocPrint(a, "{s}__migrating", .{table});
            const tmp_info = try st.query(a, "SELECT name FROM pragma_table_info(?)", &.{.{ .text = tmp }});
            const real_info = try st.query(a, "SELECT name FROM pragma_table_info(?)", &.{.{ .text = table }});
            if (tmp_info.len > 0 and real_info.len == 0) {
                try execSql(st, a, try std.fmt.allocPrint(a, "ALTER TABLE \"{s}\" RENAME TO \"{s}\";", .{ tmp, table }));
                std.debug.print("{s}: a rebuild was interrupted before its rename — adopted {s}\n", .{ table, tmp });
            }
        }
        // FINDING 9: existence — and the existing columns — are the DATABASE's to answer.
        const info = try st.query(a, "SELECT name FROM pragma_table_info(?)", &.{.{ .text = table }});
        var existing: ?[]const []const u8 = null;
        if (info.len > 0) {
            const ex = try a.alloc([]const u8, info.len);
            for (info, 0..) |row, k| ex[k] = row[0].text;
            existing = ex;
        }

        // §10dg: the shape this replica BUILT — its own record, not an introspection
        // (whose type text differs per engine). Key shape moved → re-key: the table
        // is rebuilt EMPTY (rows keyed the old way cannot be re-keyed in place, and
        // SQLite's INTEGER PRIMARY KEY refuses a uuid) and the caller re-seeds it.
        // A non-key type moved → a rebuild that keeps the rows (below).
        const key_now = try core.keyShape(a, pk, cols_v.array);
        const type_now = try core.typeShape(a, cols_v.array);
        try ensureShape(st);
        const shape_rows = try st.query(a, "SELECT key_shape, type_shape FROM _zbz_shape WHERE tbl = ?", &.{.{ .text = table }});
        const key_before: ?[]const u8 = if (shape_rows.len > 0 and shape_rows[0][0] == .text) shape_rows[0][0].text else null;
        const type_before: ?[]const u8 = if (shape_rows.len > 0 and shape_rows[0][1] == .text) shape_rows[0][1].text else null;
        // With no record (a replica from before the record, or one lost to a kill) the
        // PHYSICAL pk column names decide — names only, never the engine's type text.
        const rekey = blk: {
            if (existing == null) break :blk false;
            if (key_before) |kb| break :blk !std.mem.eql(u8, kb, key_now);
            const phys = try st.query(a, "SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk", &.{.{ .text = table }});
            if (phys.len == 0 or phys.len != pk.len) break :blk phys.len > 0;
            for (phys, 0..) |row, k| if (!std.mem.eql(u8, row[0].text, pk[k])) break :blk true;
            break :blk false;
        };
        const retyped = (try core.retypedColumns(a, if (rekey) null else type_before, cols_v.array)).array.items;

        var outcome: Migration = .unchanged;
        if (existing == null or rekey) {
            if (rekey) {
                std.debug.print("{s}: key shape changed ({s} -> {s}) — rebuilding EMPTY\n", .{ table, key_before.?, key_now });
                try execSql(st, a, try std.fmt.allocPrint(a, "DROP VIEW IF EXISTS {s}_view;", .{table}));
                try execSql(st, a, "PRAGMA foreign_keys = OFF;");
            }
            const steps = try core.createTableSteps(a, table, cols_v.array, pk, fks.array);
            for (steps.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
            if (rekey) {
                try execSql(st, a, "PRAGMA foreign_keys = ON;");
                const vsteps = try core.viewSteps(a, table, names.items);
                for (vsteps.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
            }
            outcome = if (rekey) .rekeyed else .created;
        } else {
            const diff = try core.diffColumns(a, existing, names.items, renamed);
            const renames = diff.object.get("renames").?.array.items;
            const added = diff.object.get("added").?.array.items;
            const removed = diff.object.get("removed").?.array.items;
            const fk_clauses = try core.fkClausesFor(a, fks.array);
            const ddl_rows = try st.query(a, "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?", &.{.{ .text = table }});
            const ddl: []const u8 = if (ddl_rows.len > 0 and ddl_rows[0][0] == .text) ddl_rows[0][0].text else "";
            const fk_differs = try core.fkTextDiffers(a, ddl, fk_clauses);
            const shape_changed = renames.len > 0 or added.len > 0 or removed.len > 0 or retyped.len > 0;

            if (shape_changed or fk_differs) {
                // The view goes FIRST (§1.17): DROP COLUMN re-validates every schema
                // object referencing the table, and a stale view kills the ALTER.
                try execSql(st, a, try std.fmt.allocPrint(a, "DROP VIEW IF EXISTS {s}_view;", .{table}));
                // And so do the indexes no longer published — SQLite refuses DROP
                // COLUMN on an indexed column, which sent a plain remove down the
                // rebuild path in the unit test before this line existed. Drops here,
                // creates after the shape settled (the plan below).
                const pre = try indexPlan(st, a, table, idx.array);
                for (pre.object.get("drops").?.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
                for (renames) |pair| {
                    try execSql(st, a, try std.fmt.allocPrint(a, "ALTER TABLE {s} RENAME COLUMN \"{s}\" TO \"{s}\";", .{ table, pair.array.items[0].string, pair.array.items[1].string }));
                }
                outcome = .altered;
                const altered = blk: {
                    for (removed) |n| execSql(st, a, try std.fmt.allocPrint(a, "ALTER TABLE {s} DROP COLUMN \"{s}\";", .{ table, n.string })) catch break :blk false;
                    for (added) |n| {
                        const ty = columnType(cols_v.array, n.string) orelse "TEXT";
                        // A constant default rides along (§10df): SQLite fills the existing
                        // rows with it, so PostgreSQL's old rows and ours converge.
                        const dflt: []const u8 = if (columnDefault(cols_v.array, n.string)) |d| try std.fmt.allocPrint(a, " DEFAULT {s}", .{d}) else "";
                        execSql(st, a, try std.fmt.allocPrint(a, "ALTER TABLE {s} ADD COLUMN \"{s}\" {s}{s};", .{ table, n.string, ty, dflt })) catch break :blk false;
                    }
                    // SQLite has no ALTER TABLE ADD/DROP CONSTRAINT: an FK change is a rebuild.
                    if (fk_differs) break :blk false;
                    // Nor ALTER COLUMN TYPE (§10dg): a re-typed column is a rebuild that
                    // copies the rows — affinity converts what converts.
                    if (retyped.len > 0) {
                        std.debug.print("{s}: {d} column(s) re-typed — rebuilding, rows kept\n", .{ table, retyped.len });
                        break :blk false;
                    }
                    break :blk true;
                };
                if (!altered) {
                    // Schema surgery on a consistent copy: with foreign_keys ON the DROP
                    // of a referenced parent is refused (measured: users, blocked by
                    // salaries' FK). Off for the surgery, on after — data is copied.
                    try execSql(st, a, "PRAGMA foreign_keys = OFF;");
                    const steps = try core.rebuildSteps(a, table, cols_v.array, pk, fks.array, existing.?);
                    const carried = blk: {
                        for (steps.array.items) |stp| {
                            const sql = stp.object.get("sql").?.string;
                            execSql(st, a, sql) catch {
                                std.debug.print("{s}: rows could not be carried through the rebuild ({s}) at: {s}\n", .{ table, st.errMsg(), sql });
                                break :blk false;
                            };
                        }
                        break :blk true;
                    };
                    if (!carried) {
                        // §10dg: a migration that cannot carry the rows degrades to a
                        // re-seed, never to a table stuck in its old shape — measured on
                        // a re-key: the parent stood empty (waiting for its full) while
                        // the child's copy hit its FOREIGN KEY. Empty now; the caller
                        // drops the watermark and the next full brings the rows back.
                        try execSql(st, a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS \"{s}__migrating\";", .{table}));
                        const fresh = try core.createTableSteps(a, table, cols_v.array, pk, fks.array);
                        for (fresh.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
                    }
                    try execSql(st, a, "PRAGMA foreign_keys = ON;");
                    outcome = if (carried) .rebuilt else .emptied;
                }
                const vsteps = try core.viewSteps(a, table, names.items);
                for (vsteps.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
            }
        }

        // Indexes on every path — after a rebuild, because the DROP took them along.
        const plan = try indexPlan(st, a, table, idx.array);
        for (plan.object.get("drops").?.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
        for (plan.object.get("creates").?.array.items) |stp| try execSql(st, a, stp.object.get("sql").?.string);
        // The shape record, on every path (§10dg) — including the first sight, so the
        // NEXT descriptor has something to be compared with.
        _ = try st.query(a, "INSERT INTO _zbz_shape (tbl, key_shape, type_shape) VALUES (?, ?, ?) ON CONFLICT(tbl) DO UPDATE SET key_shape = excluded.key_shape, type_shape = excluded.type_shape", &.{ .{ .text = table }, .{ .text = key_now }, .{ .text = type_now } });
        return outcome;
    }

    /// `core.indexSyncPlan` over what the database actually holds (finding 9 again).
    fn indexPlan(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, want: std.json.Array) !Value {
        const have_rows = try st.query(a, "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name = ? AND name NOT LIKE 'sqlite_%'", &.{.{ .text = table }});
        const have = try a.alloc([]const u8, have_rows.len);
        for (have_rows, 0..) |row, k| have[k] = row[0].text;
        return core.indexSyncPlan(a, table, have, want);
    }

    /// The `sqlite.columns` array of a descriptor, or null for anything that is not a
    /// usable descriptor (a suspension, a non-object, a missing key).
    fn descriptorColumns(val: Value) ?Value {
        if (val != .object) return null;
        const sq = val.object.get("sqlite") orelse return null;
        if (sq != .object) return null;
        const cols = sq.object.get("columns") orelse return null;
        if (cols != .array) return null;
        return cols;
    }

    fn columnDefault(cols: std.json.Array, name: []const u8) ?[]const u8 {
        for (cols.items) |c| {
            if (c != .object) continue;
            const n = c.object.get("name") orelse continue;
            if (n != .string or !std.mem.eql(u8, n.string, name)) continue;
            const d = c.object.get("default") orelse return null;
            return if (d == .string and d.string.len > 0) d.string else null;
        }
        return null;
    }

    fn columnType(cols: std.json.Array, name: []const u8) ?[]const u8 {
        for (cols.items) |c| {
            if (std.mem.eql(u8, c.object.get("name").?.string, name)) {
                return if (c.object.get("type")) |t| (if (t == .string) t.string else null) else null;
            }
        }
        return null;
    }

    fn execSql(st: *storage.Storage, a: std.mem.Allocator, sql: []const u8) !void {
        _ = try st.query(a, sql, &.{});
    }

    /// Every table's descriptor from KV through `migrateTable`, then the in-memory
    /// state (pk, columns, route…) refreshed only when it changed — this runs on
    /// every `syncOnce`, and the client-lifetime arena must not grow on a no-op.
    pub fn syncSchemas(self: *SyncClient) !void {
        var sa = std.heap.ArenaAllocator.init(self.a);
        defer sa.deinit();
        const a = sa.allocator();
        for (self.opts.tables) |table| {
            const bytes = (try self.t.kvGet(a, self.kv_schemas, table)) orelse {
                std.debug.print("schema missing for {s}\n", .{table});
                continue;
            };
            const val = (try std.json.parseFromSlice(Value, a, bytes, .{})).value;
            try self.applyDescriptor(a, table, val);
        }
        try self.st.execSimple("CREATE TABLE IF NOT EXISTS _zbz_stream_seq (stream TEXT PRIMARY KEY, last_seq INTEGER NOT NULL)");
        try ensureInbox(&self.st);
        try self.st.execSimple("CREATE TABLE IF NOT EXISTS _zbz_generations (tbl TEXT PRIMARY KEY, watermark TEXT, cutoff_lsn INTEGER, seed_epoch INTEGER NOT NULL DEFAULT 0)");
        try ensureShape(&self.st);
        self.st.execSimple("ALTER TABLE _zbz_generations ADD COLUMN seed_epoch INTEGER NOT NULL DEFAULT 0") catch {}; // a replica from before §10df
        // A schema that just moved may be what a held event was waiting for.
        self.retryHeld(null, null);
    }

    /// One descriptor onto one table: migrate the physical table and refresh the
    /// TableState — or, on a tombstone, drop the local table (the TS `dropLocalTable`).
    /// Shared by `syncSchemas` (every table, on sync) and `drainSchemaWatch` (whatever
    /// moved, on poll).
    fn applyDescriptor(self: *SyncClient, a: std.mem.Allocator, table: []const u8, val: Value) !void {
        const outcome = migrateTable(&self.st, a, table, val) catch |err| switch (err) {
            error.SchemaUnusable => {
                // Suspended (no primary key, or unrouted — §5) or malformed: the
                // table stays as it is locally, and stays out of `states` if it was
                // never usable, so no CDC event is applied against a missing table.
                const why = if (val == .object) (val.object.get("suspended") orelse Value{ .null = {} }) else Value{ .null = {} };
                std.debug.print("{s}: schema unusable ({s}) — skipped\n", .{ table, if (why == .string) why.string else "no columns" });
                // Dropped upstream: whatever was held for it will never find a parent.
                const dropped = if (val == .object) (val.object.get("dropped") orelse Value{ .null = {} }) else Value{ .null = {} };
                if (dropped == .bool and dropped.bool) try self.dropLocalTable(a, table);
                return;
            },
            else => return err,
        };
        if (outcome != .unchanged) std.debug.print("{s}: {s}\n", .{ table, @tagName(outcome) });
        if (outcome == .rekeyed or outcome == .emptied or outcome == .created) {
            // §10dg: the rows are gone (with the old key, or because the rebuild could
            // not carry them); so is everything that referred to them — the watermark
            // (the next gap check seeds a fresh full) and the events held for the
            // table (keyed the old way, they can never apply). `.created` too (§10dj):
            // a table that did not exist cannot be seeded, whatever a watermark left
            // behind by a kill says.
            // `catch`: on the very first sync the bookkeeping tables are created AFTER
            // this loop, and a table with no bookkeeping has no watermark to drop.
            _ = self.st.query(a, "DELETE FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }}) catch {};
            pruneInboxDropped(&self.st, a, table) catch {};
            self.reseed_pending = true;
            std.debug.print("{s}: {s} — watermark dropped, re-seeding from a fresh full\n", .{ table, @tagName(outcome) });
        }

        const pk = try jsonStrList(a, val.object.get("pk_columns"));
        const cols_v = descriptorColumns(val).?; // checked by migrateTable above
        var names: std.ArrayList([]const u8) = .empty;
        for (cols_v.array.items) |c| try names.append(a, c.object.get("name").?.string);
        const tenant_col: ?[]const u8 = if (val.object.get("tenant_column")) |v| (if (v == .string) v.string else null) else null;
        const version_col: ?[]const u8 = if (val.object.get("version_column")) |v| (if (v == .string) v.string else null) else null;
        const tombstone_col: ?[]const u8 = if (val.object.get("tombstone_column")) |v| (if (v == .string) v.string else null) else null;
        const seed_epoch: i64 = if (val.object.get("seed_epoch")) |v| (if (v == .integer) v.integer else 0) else 0;

        if (self.states.getPtr(table)) |st| {
            // §10df first: a republished descriptor is usually UNCHANGED in shape — the
            // epoch is the whole message, and it must not be lost to the shortcut below.
            st.seed_epoch = seed_epoch;
            try self.reseedIfEpochMoved(a, table, seed_epoch);
            if (outcome == .unchanged and sameStrings(st.cols, names.items) and sameStrings(st.pk, pk)) return;
        }
        if (tenant_col != null and self.tenant_missing) {
            std.debug.print("{s}: tenant-scoped and '{s}' has no tenant — not followed (the local rows stay as they are)\n", .{ table, self.opts.principal });
            return;
        }
        // Changed, or first time: into the client-lifetime arena.
        const ca = self.aa();
        // The OPEN tenant's rows ride the public stream (`cdc.<open>.>` is one of its
        // subjects); a principal mapped to it must not look for a `CDC_<open>` stream.
        const route = if (tenant_col != null and !std.mem.eql(u8, self.tenant, self.open_tenant))
            try std.fmt.allocPrint(ca, "{s}{s}", .{ self.cdc_prefix, self.tenant })
        else
            self.cdc_public;
        const shared_route: ?[]const u8 = if (tenant_col != null) self.cdc_public else null;
        const fresh: TableState = .{
            .pk = try dupeStrings(ca, pk),
            .cols = try dupeStrings(ca, names.items),
            .version_col = if (version_col) |v| try ca.dupe(u8, v) else null,
            .tenant_col = if (tenant_col) |v| try ca.dupe(u8, v) else null,
            .tombstone_col = if (tombstone_col) |v| try ca.dupe(u8, v) else null,
            .route = route,
            .shared_route = shared_route,
            .seed_epoch = seed_epoch,
        };
        if (self.states.getPtr(table)) |st| {
            // In place: the seed gate (`seed_seq/seed_stream/seed_lsn`) belongs to
            // the replica's history, not to the descriptor, and survives a migration.
            st.pk = fresh.pk;
            st.cols = fresh.cols;
            st.version_col = fresh.version_col;
            st.tenant_col = fresh.tenant_col;
            st.tombstone_col = fresh.tombstone_col;
            st.route = fresh.route;
            st.shared_route = fresh.shared_route;
            st.seed_epoch = fresh.seed_epoch;
        } else {
            try self.states.put(ca, try ca.dupe(u8, table), fresh);
            // Born under the live watch (enabled after this client connected): it has
            // no watermark and nothing asked for its seed — the next poll does.
            self.reseed_pending = true;
        }
    }

    /// §10df: the descriptor's seed_epoch is above the one this replica seeded at —
    /// zebridge_reseed() ran upstream. Forget the watermark; `gapAndSeed` (next sync,
    /// or the end of this poll's schema drain) seeds a fresh full.
    fn reseedIfEpochMoved(self: *SyncClient, a: std.mem.Allocator, table: []const u8, epoch: i64) !void {
        const rows = self.st.query(a, "SELECT seed_epoch FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }}) catch return;
        if (rows.len == 0) {
            // Never seeded (no chain at connect, or one the replica could not use):
            // nothing to drop, but the next poll must ask again — measured: a table
            // following CDC unseeded stayed that way through an epoch move.
            self.reseed_pending = true;
            return;
        }
        const stored: i64 = if (rows[0][0] == .integer) rows[0][0].integer else 0;
        if (stored >= epoch) return;
        _ = try self.st.query(a, "DELETE FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }});
        std.debug.print("{s}: seed epoch {d} -> {d} (zebridge_reseed) — watermark dropped, re-seeding from a fresh full\n", .{ table, stored, epoch });
        self.reseed_pending = true;
    }

    /// The table is gone upstream: drop it here, forget its state, discard what was
    /// held for it (the TS `dropLocalTable`). Stale rows must not stay readable as
    /// if they were live.
    fn dropLocalTable(self: *SyncClient, a: std.mem.Allocator, table: []const u8) !void {
        pruneInboxDropped(&self.st, a, table) catch {};
        try execSql(&self.st, a, try std.fmt.allocPrint(a, "DROP VIEW IF EXISTS \"{s}_view\";", .{table}));
        try execSql(&self.st, a, try std.fmt.allocPrint(a, "DROP TABLE IF EXISTS \"{s}\";", .{table}));
        _ = self.states.orderedRemove(table);
        std.debug.print("{s}: dropped locally — the table was dropped upstream\n", .{table});
    }

    /// Drain the schemas watch: every descriptor that changed since the last poll,
    /// applied through the same path `sync()` uses. Non-blocking (1 ms). Opens the
    /// watch on first use — after the grammar named the bucket.
    fn drainSchemaWatch(self: *SyncClient, report_a: std.mem.Allocator, changed_map: *std.StringArrayHashMapUnmanaged(void), seeded_map: *std.StringArrayHashMapUnmanaged(void)) !void {
        if (self.schema_watch == null) {
            self.schema_kv = try self.t.js.kvBucket(self.kv_schemas);
            self.schema_watch = try self.schema_kv.?.watchAll(.{ .updates_only = true });
        }
        var sa = std.heap.ArenaAllocator.init(self.a);
        defer sa.deinit();
        const a = sa.allocator();
        var moved: usize = 0;
        const t: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } };
        while (true) {
            var entry = (self.schema_watch.?.next(t) catch |err| switch (err) {
                error.Timeout => break,
                else => return err,
            }) orelse break;
            defer entry.deinit();
            const mine = for (self.opts.tables) |tb| {
                if (std.mem.eql(u8, tb, entry.key)) break true;
            } else false;
            if (!mine or entry.isDeleted()) continue;
            const val = std.json.parseFromSliceLeaky(Value, a, entry.value, .{}) catch continue;
            const key = try a.dupe(u8, entry.key);
            self.applyDescriptor(a, key, val) catch |err| {
                std.debug.print("{s}: live schema not applied: {s}\n", .{ key, @errorName(err) });
                continue;
            };
            moved += 1;
        }
        if (moved > 0) self.retryHeld(report_a, changed_map);
        if (self.reseed_pending) {
            self.reseed_pending = false;
            self.gapAndSeed(report_a, seeded_map) catch |err| std.debug.print("re-seed after epoch move: {s}\n", .{@errorName(err)});
        }
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
        const rows = try self.st.query(qa.allocator(), "SELECT last_seq FROM _zbz_stream_seq WHERE stream = ?", &.{.{ .text = stream }});
        if (rows.len == 0) return 0;
        return @intCast(rows[0][0].integer);
    }

    fn persistSeq(self: *SyncClient, stream: []const u8, seq: u64) !void {
        var qa = std.heap.ArenaAllocator.init(self.a);
        defer qa.deinit();
        _ = try self.st.query(qa.allocator(), "INSERT INTO _zbz_stream_seq (stream, last_seq) VALUES (?, ?) ON CONFLICT(stream) DO UPDATE SET last_seq = excluded.last_seq", &.{ .{ .text = stream }, .{ .integer = @intCast(seq) } });
    }

    fn effTenant(self: *SyncClient, table: []const u8) []const u8 {
        const st = self.states.get(table) orelse return self.open_tenant;
        return if (st.tenant_col != null) self.tenant else self.open_tenant;
    }

    // ─── step 2: the gap rule (per stream) + scoped seeding (§10n) ──────────

    pub fn gapAndSeed(self: *SyncClient, report_a: ?std.mem.Allocator, seeded_map: ?*std.StringArrayHashMapUnmanaged(void)) !void {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        var gapped: std.StringArrayHashMapUnmanaged(void) = .empty;
        // Every stream any table depends on — a tenant-scoped table names two (§10bq),
        // and a client with no public table would otherwise never inspect CDC_PUBLIC at
        // all, so its shared rows could fall off the back unnoticed.
        var streams: std.StringArrayHashMapUnmanaged(void) = .empty;
        var sit = self.states.iterator();
        while (sit.next()) |e| {
            try streams.put(a, e.value_ptr.route, {});
            if (e.value_ptr.shared_route) |sr| try streams.put(a, sr, {});
        }
        var it = streams.iterator();
        while (it.next()) |e| {
            const stream = e.key_ptr.*;
            if (gapped.contains(stream)) continue;
            var info = self.t.js.getStreamInfo(stream) catch continue;
            defer info.deinit();
            const first: i64 = @intCast(info.value.state.first_seq);
            const last: i64 = @intCast(info.value.state.last_seq);
            const stored: i64 = @intCast(try self.storedSeq(stream));
            if (core.streamHasGap(first, stored, last)) {
                try gapped.put(a, stream, {});
                // The feed restarted under us (position beyond last_seq): the position is
                // meaningless in the new numbering. Reset it, or `fullPredates` reads the
                // fresh chain's small cutoff_seq as "older than where I am" and skips the
                // very full this gap needs (measured: slot_loss.py, 2026-08-29).
                if (last >= 0 and stored > last) {
                    std.debug.print("{s}: stream restarted (position {d} beyond last_seq {d}) — position reset\n", .{ stream, stored, last });
                    try self.persistSeq(stream, 0);
                }
            }
        }
        // Seed with foreign_keys OFF — the standard bulk-load shape, and the same
        // one the TS client uses (dialect.deferForeignKeys + foreignKeyViolations).
        // Order alone cannot save this: chains are per-table snapshots built at
        // DIFFERENT cutoffs, so a child's chain can reference a parent born after
        // the parent's chain was cut — a fresh client seeding an orders chain died
        // at row 1 on the FK, rolling back the whole step (§10cp finding 2). A seed
        // is a bulk load of almost-consistent snapshots; the tail reconciles the
        // difference, and the post-load check makes any residue LOUD instead of
        // fatal. The pragma sits OUTSIDE the per-step transactions because SQLite
        // silently ignores it inside one.
        try execSql(&self.st, a, "PRAGMA foreign_keys = OFF;");
        defer execSql(&self.st, a, "PRAGMA foreign_keys = ON;") catch {};
        // Still the CONFIGURED order (parents first — cheapest path to zero
        // residue), scoped to gapped routes plus never-seeded tables (§10n).
        for (self.opts.tables) |table| {
            const st = self.states.get(table) orelse continue;
            // ⚠️ `try`, not "treat a failed read as never seeded": that would answer a
            // locked database with a full re-seed (the same trap as `storedSeq`).
            const seeded = (try self.st.query(a, "SELECT tbl FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }})).len > 0;
            const shared_gapped = if (st.shared_route) |sr| gapped.contains(sr) else false;
            if (gapped.contains(st.route) or shared_gapped or !seeded) {
                // One table's failure is one table's failure (the TS rule): the rest
                // still seed, and the next poll's gap check retries this one.
                self.applyChain(table) catch |err| {
                    std.debug.print("{s}: seeding failed: {s} — retried at the next poll\n", .{ table, @errorName(err) });
                    continue;
                };
                if (report_a) |ra_| if (seeded_map) |sm| {
                    if (!sm.contains(table)) sm.put(ra_, ra_.dupe(u8, table) catch table, {}) catch {};
                };
            }
        }
        // §10dg: whatever kept a table unseeded — no chain yet, a chain that predates
        // the replica or the re-seed, a shape the replica lacks, a failed step — the
        // next poll asks again. Unseeded is not synced (the TS rule), and a poll loop
        // that never re-asks would follow CDC on an empty table forever.
        for (self.opts.tables) |table| {
            if (self.states.get(table) == null) continue;
            const seeded_now = (self.st.query(a, "SELECT tbl FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }}) catch continue).len > 0;
            if (!seeded_now) {
                self.reseed_pending = true;
                break;
            }
        }
        const orphans = try self.st.query(a, "PRAGMA foreign_key_check;", &.{});
        if (orphans.len > 0) {
            std.debug.print(
                "⚠️ {d} foreign key violation(s) survive seeding — cross-chain skew; the tail reconciles or holds them\n",
                .{orphans.len},
            );
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

        // §10df: a manifest built BEFORE the re-seed was asked for cannot serve it —
        // seeding from it would record the new epoch over the old data. The descriptor
        // (the epoch we hold) and the producer's full arrive on independent clocks;
        // wait for the manifest to catch up, retried on the next poll or sync.
        const man_epoch: i64 = if (man.object.get("seed_epoch")) |v| (if (v == .integer) v.integer else 0) else 0;
        const want_epoch: i64 = if (self.states.get(table)) |s0| s0.seed_epoch else 0;
        if (man_epoch < want_epoch) {
            std.debug.print("{s}: chain g{d} predates the re-seed (epoch {d} < {d}) — waiting for the producer's full\n", .{ table, if (man.object.get("gen")) |v| v.integer else 0, man_epoch, want_epoch });
            self.reseed_pending = true;
            return;
        }

        const wm_rows = try self.st.query(a, "SELECT watermark FROM _zbz_generations WHERE tbl = ?", &.{.{ .text = table }});
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
            // A chain object that is not the shape the producer writes (a corrupt or
            // foreign object under the name) is an ERROR the host sees — never a union
            // access that kills the host process (measured: five stray bytes decoded
            // as an integer, then read as an object).
            if (doc != .object) return error.ChainObjectMalformed;
            const rows_v = doc.object.get("rows") orelse return error.ChainObjectMalformed;
            if (rows_v != .array) return error.ChainObjectMalformed;
            const cols = try jsonStrList(step_a, doc.object.get("columns"));
            // §10dg: a chain object built from an OLDER shape names columns this table
            // no longer has (a table dropped and reborn under its name, a DROP/RENAME
            // the producer has not rebuilt for yet). Not an error: wait for its full.
            for (cols) |col| if (!contains(st.cols, col)) {
                std.debug.print("{s}: chain object {s} names column {s}, which the replica lacks — predates the schema, waiting for the producer's full\n", .{ table, step.object.get("name").?.string, col });
                return;
            };
            const vcol_v = doc.object.get("version_column") orelse (man.object.get("version_column") orelse @as(Value, .null));
            const vcol: ?[]const u8 = if (vcol_v == .string and contains(cols, vcol_v.string)) vcol_v.string else null;
            const rows = rows_v.array;
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
                    table,          step.object.get("name").?.string, if (cs.is_full) "full" else "delta",
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
        _ = try self.st.query(a, "INSERT INTO _zbz_generations (tbl, watermark, cutoff_lsn, seed_epoch) VALUES (?, ?, ?, ?) ON CONFLICT(tbl) DO UPDATE SET watermark = excluded.watermark, cutoff_lsn = excluded.cutoff_lsn, seed_epoch = excluded.seed_epoch", &.{ .{ .text = table }, .{ .text = cv }, .{ .integer = st.seed_lsn orelse 0 }, .{ .integer = st.seed_epoch } });
        // A held event at or below the seed's LSN is inside the chain just applied:
        // superseded, not waiting (the TS client's pruneInboxSeeded).
        try pruneInboxSeeded(&self.st, a, table, st.seed_lsn orelse 0);
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
        self.retryHeld(null, null);
    }

    /// The streams this client reads, public first: parents (users) ride
    /// CDC_PUBLIC — fewer FK holds. Allocated from `a`.
    fn cdcStreams(self: *SyncClient, a: std.mem.Allocator) ![]const []const u8 {
        var streams: std.StringArrayHashMapUnmanaged(void) = .empty;
        var it = self.states.iterator();
        while (it.next()) |e| {
            if (std.mem.eql(u8, e.value_ptr.route, self.cdc_public)) try streams.put(a, e.value_ptr.route, {});
        }
        it = self.states.iterator();
        while (it.next()) |e| try streams.put(a, e.value_ptr.route, {});
        return try a.dupe([]const u8, streams.keys());
    }

    /// FK hold/retry (§10h): one bulk retry pass after every stream had its turn.
    /// The inbox is the truth (the TS client's `_zebridge_inbox`, ported — CLIENTS.md):
    /// every held event lives in `_zbz_inbox` from the batch that held it until the
    /// retry that lands it. Still missing its parent → attempts + 1, next pass. Any
    /// other failure → dropped, loudly: an event that can never apply must not sit
    /// forever pretending it will.
    /// Passes until a pass resolves nothing (§10dg): a child held behind a parent that
    /// is itself held behind a grandparent needs the second pass — measured, the
    /// single pass left the child in the inbox until an unrelated event arrived.
    fn retryHeld(self: *SyncClient, report_a: ?std.mem.Allocator, changed_map: ?*std.StringArrayHashMapUnmanaged(void)) void {
        var passes: usize = 0;
        var total_len: usize = 0;
        var total_resolved: usize = 0;
        var total_dropped: usize = 0;
        // Limit passes to break cycles if two rows wait for each other.
        while (passes < 16) : (passes += 1) {
            const r = self.retryHeldPass(report_a, changed_map) catch return;
            if (passes == 0) total_len = r.len;
            total_resolved += r.resolved;
            total_dropped += r.dropped;
            if (r.resolved == 0 or r.len == r.resolved + r.dropped) break;
        }
        if (total_resolved > 0 or total_dropped > 0) {
            std.debug.print("fk held: {d}, applied on retry: {d}, dropped: {d}, still waiting: {d} ({d} pass(es))\n", .{ total_len, total_resolved, total_dropped, total_len -| (total_resolved + total_dropped), passes + 1 });
        }
    }

    const RetryPass = struct { len: usize, resolved: usize, dropped: usize };

    fn retryHeldPass(self: *SyncClient, report_a: ?std.mem.Allocator, changed_map: ?*std.StringArrayHashMapUnmanaged(void)) !RetryPass {
        var ra = std.heap.ArenaAllocator.init(self.a);
        defer ra.deinit();
        const a = ra.allocator();
        const rows = try self.st.query(a, "SELECT id, tbl, ev FROM _zbz_inbox ORDER BY id", &.{});
        if (rows.len == 0) return .{ .len = 0, .resolved = 0, .dropped = 0 };
        var resolved: usize = 0;
        var dropped: usize = 0;
        for (rows) |r| {
            const id = r[0];
            const table = r[1].text;
            const ev = std.json.parseFromSliceLeaky(Value, a, r[2].text, .{}) catch {
                _ = self.st.query(a, "DELETE FROM _zbz_inbox WHERE id = ?", &.{id}) catch {};
                dropped += 1;
                continue;
            };
            if (self.applyEvent(table, ev, 0)) |_| {
                _ = self.st.query(a, "DELETE FROM _zbz_inbox WHERE id = ?", &.{id}) catch {};
                resolved += 1;
                // Duped: `table` is a row of this pass's arena (§10ee).
                if (report_a) |ra_| if (changed_map) |cm| {
                    if (!cm.contains(table)) cm.put(ra_, ra_.dupe(u8, table) catch table, {}) catch {};
                };
            } else |err| switch (err) {
                error.FkHeld, error.SchemaBehind => {
                    _ = self.st.query(a, "UPDATE _zbz_inbox SET attempts = attempts + 1 WHERE id = ?", &.{id}) catch {};
                },
                else => {
                    std.debug.print("DROPPED held event on {s}: {s}\n", .{ table, @errorName(err) });
                    _ = self.st.query(a, "DELETE FROM _zbz_inbox WHERE id = ?", &.{id}) catch {};
                    dropped += 1;
                },
            }
        }
        return .{ .len = rows.len, .resolved = resolved, .dropped = dropped };
    }

    /// A pull consumer on `stream` positioned just past the stored sequence, named
    /// uniquely per process, reaped by the server after `inactive_ns` idle.
    fn openConsumer(self: *SyncClient, stream: []const u8, inactive_ns: u64, shared: ?*@import("nats").PullInbox) !*@import("nats").PullSubscription {
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
        cfg.inactive_threshold = inactive_ns;
        if (last > 0) {
            cfg.deliver_policy = .by_start_sequence;
            cfg.opt_start_seq = last + 1;
        } else {
            cfg.deliver_policy = .all;
        }
        return try self.t.js.pullSubscribe(null, cname, .{ .stream = stream, .config = cfg, .inbox = shared });
    }

    fn drainStream(self: *SyncClient, stream: []const u8) !void {
        const last = try self.storedSeq(stream);
        var sub = try self.openConsumer(stream, 30 * std.time.ns_per_s, null);
        defer sub.deinit(); // the server reaps the consumer itself — see openConsumer

        var max_seq: u64 = last;
        // ONE reopen if the consumer dies under us mid-drain (a 409 under 12
        // concurrent seeding clients, §10cq): the position is the client's, so a
        // fresh consumer resumes exactly where the last batch left off. A second
        // death is a real fault and propagates.
        var reopened = false;
        while (true) {
            // ⚠️ Only a TIMEOUT means "caught up". A closed connection or a slow
            // consumer used to break here too — and then persist the position, which
            // is how a network blip becomes a recorded claim to have read the tail.
            //
            // ⚠️ This arm was DEAD CODE until 2026-08-29 (§10bh): the unpatched
            // nats.zig `fetch` never returned `Timeout` — it handed back an empty batch
            // with the error parked in `batch.err`, and the length check below was the
            // only thing ending the drain. Against the patched `fetch`
            // (`nats.zig-fetch-early-return-503.patch`) an empty batch with a terminal
            // status IS a returned error, so this arm is the exit and the length check
            // is a belt for a partial-batch `fetch` that returns nothing (it cannot).
            var batch = sub.fetch(100, .{ .duration = .{ .raw = .fromMilliseconds(900), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => break, // the expiry: caught up to the tail
                error.ConsumerSequenceMismatch, error.NoResponders => {
                    if (reopened) return err;
                    reopened = true;
                    const fresh = try self.openConsumer(stream, 30 * std.time.ns_per_s, null);
                    sub.deinit();
                    sub = fresh;
                    continue;
                },
                else => return err,
            };
            defer batch.deinit();
            if (batch.messages.len == 0) break;
            _ = try self.applyBatch(null, stream, batch.messages, last, &max_seq, null);
        }
        if (max_seq > last) try self.persistSeq(stream, max_seq);
        std.debug.print("{s}: drained to seq {d}\n", .{ stream, max_seq });
    }

    /// One fetched batch through the gate → apply → hold → position path, shared by
    /// the bounded drain and the live tail. Returns the number of events offered to
    /// `applyEvent` (applied, gated or held — D1: all three ARE the position).
    fn applyBatch(self: *SyncClient, report_a: ?std.mem.Allocator, stream: []const u8, messages: []const *@import("nats").JetStreamMessage, last: u64, max_seq: *u64, changed_map: ?*std.StringArrayHashMapUnmanaged(void)) !usize {
        // Per-batch: every decoded event dies with the batch, except the FK-held
        // ones, which are held DURABLY in `_zbz_inbox` below (§10de finding 1).
        var ba = std.heap.ArenaAllocator.init(self.a);
        defer ba.deinit();

        // ONE transaction for the whole batch — the same lesson the TS client has
        // in writing: N autocommits each pay a full SQLite commit/fsync, one
        // transaction of N pays it once. Row-by-row, a swarm client applied ~7
        // events/s against a 19/s feed and fell 76 s behind by the audit (§10cq);
        // the position write rides the same transaction, so data and position
        // cannot disagree across a crash. Acks stay OUTSIDE, after COMMIT: an
        // acked-but-rolled-back batch would be lost, an unacked-but-committed one
        // merely redelivers into idempotent upserts.
        const Ctx = struct {
            client: *SyncClient,
            a: std.mem.Allocator,
            stream: []const u8,
            messages: []const *@import("nats").JetStreamMessage,
            last: u64,
            max_seq: *u64,
            offered: *usize,
            report_a: ?std.mem.Allocator,
            changed_map: ?*std.StringArrayHashMapUnmanaged(void),
            /// §10dg: FOREIGN KEY checks deferred to COMMIT for the whole batch (the TS
            /// client's `defer_foreign_keys`): a family inserted child-first, or a
            /// cascade's deletes arriving parent-first, lands as one unit with no hold at
            /// all. A COMMIT refused (a parent missing across BATCHES) rolls back and the
            /// caller replays with immediate checks, holding what cannot land.
            deferred: bool,
            fn apply(cx: @This(), st_: *storage.Storage) !void {
                if (cx.deferred) try st_.execSimple("PRAGMA defer_foreign_keys = ON;");
                for (cx.messages) |m| {
                    const seq = m.metadata.sequence.stream;
                    const doc = decodeMsgpack(cx.a, m.msg.data) catch continue;
                    const events: []const Value = if (doc == .array) doc.array.items else &.{doc};
                    for (events) |ev| {
                        if (ev != .object) continue;
                        const table = if (ev.object.get("table")) |v| (if (v == .string) v.string else continue) else continue;
                        if (cx.client.states.get(table) == null) continue;
                        cx.offered.* += 1;
                        var applied_here = true;
                        cx.client.applyEvent(table, ev, seq) catch |err| switch (err) {
                            // An OOM here is a `try`, not a `catch {}`: a dropped hold is an
                            // event that is acked, positioned past, and never applied.
                            // Held DURABLY, in this batch's transaction (CLIENTS.md, §10de
                            // finding 1): the position below is persisted past this event,
                            // so the inbox is the only thing that remembers it.
                            error.FkHeld => {
                                applied_here = false;
                                try holdEvent(st_, cx.a, table, ev, "missing-parent");
                            },
                            error.SchemaBehind => {
                                applied_here = false;
                                try holdEvent(st_, cx.a, table, ev, "unknown-column");
                            },
                            // Anything else is an event acked, positioned past and never
                            // applied — it must at least be SAID. The SQLite text names the cause.
                            else => |e| {
                                applied_here = false;
                                std.debug.print("{s}: event at seq {d} not applied: {s} — sqlite: {s}\n", .{ table, seq, @errorName(e), st_.errMsg() });
                            },
                        };
                        // The host's `changed_tables` (§10ee): only what was APPLIED — a
                        // held event changed nothing yet — and the name DUPED into the
                        // report's allocator: `table` lives in this batch's arena, which
                        // dies before the host reads the report.
                        if (applied_here) if (cx.report_a) |ra_| if (cx.changed_map) |cm| {
                            if (!cm.contains(table)) cm.put(ra_, ra_.dupe(u8, table) catch table, {}) catch {};
                        };
                    }
                    if (seq > cx.max_seq.*) cx.max_seq.* = seq;
                }
                if (cx.max_seq.* > cx.last) try cx.client.persistSeq(cx.stream, cx.max_seq.*);
            }
        };
        var offered: usize = 0;
        var ctx = Ctx{
            .client = self,
            .a = ba.allocator(),
            .stream = stream,
            .messages = messages,
            .last = last,
            .max_seq = max_seq,
            .offered = &offered,
            .report_a = report_a,
            .changed_map = changed_map,
            .deferred = true,
        };
        self.st.transaction(ctx, Ctx.apply) catch |err| {
            std.debug.print("{s}: batch of {d} message(s) refused as a unit ({s}: {s}) — replaying event by event, holding what cannot land\n", .{ stream, messages.len, @errorName(err), self.st.commitErr() });
            offered = 0;
            max_seq.* = last;
            ctx.deferred = false;
            try self.st.transaction(ctx, Ctx.apply);
        };
        for (messages) |m| m.ack() catch {};
        return offered;
    }

    // ─── live tailing (§10bh): the host-driven poll ─────────────────────────

    /// The tail for `stream`, opened on first use.
    fn tailInbox(self: *SyncClient) !*@import("nats").PullInbox {
        if (self.tail_inbox) |ib| return ib;
        self.tail_inbox = try self.t.js.pullInbox();
        return self.tail_inbox.?;
    }

    fn tailFor(self: *SyncClient, stream: []const u8) !*Tail {
        for (self.tails.items) |*t| {
            if (std.mem.eql(u8, t.stream, stream)) return t;
        }
        const sub = try self.openConsumer(stream, tail_inactive_ns, try self.tailInbox());
        errdefer sub.deinit();
        try self.tails.append(self.aa(), .{ .stream = stream, .sub = sub });
        return &self.tails.items[self.tails.items.len - 1];
    }

    /// The consumer behind a tail is gone (reaped, or deleted): open a fresh one at
    /// the stored position. Positions are the client's, not the consumer's (D1), so
    /// nothing is lost — the new consumer starts where the replica actually is.
    fn reopenTail(self: *SyncClient, t: *Tail) !void {
        // OPEN-THEN-SWAP. Opening can fail — a JS API request mid-outage times out —
        // and deinit-first left `t.sub` DANGLING on exactly that failure: every later
        // poll handed the freed pointer to PullInbox.fetch, which refused it
        // (NotOnThisInbox) for the rest of the process — the silent tail wedge of
        // §10cp/§10cq, measured as one Timeout then 154 refusals while verdicts kept
        // flowing on the same connection. The old consumer needs no explicit delete:
        // the server reaps it via inactive_threshold.
        const fresh = try self.openConsumer(t.stream, tail_inactive_ns, try self.tailInbox());
        t.sub.deinit();
        t.sub = fresh;
    }

    pub const PollReport = struct { applied: usize, settled: usize, changed_tables: []const []const u8, seeded: []const []const u8 };

    /// One turn of the host's loop: wait up to `wait_ms` for CDC on the persistent
    /// tails, apply what arrived, retry the FK-held, then sweep verdicts without
    /// waiting. Blocks the calling thread only — the host owns the thread, which is
    /// the C-ABI-honest shape (§10bh). The wait is shared across the streams in turn;
    /// once anything arrives the remaining streams get a quick look, not the full wait.
    /// The server's own auth verdict, if the connection was terminally closed for
    /// one (§10cj). Consulted by the C ABI so a poll that fails on a dead socket is
    /// named "AuthExpired"/"AuthRevoked" — actionable — not a bare transport error.
    pub fn authError(self: *SyncClient) ?anyerror {
        return self.t.conn.lastAuthError();
    }

    pub fn poll(self: *SyncClient, report_a: std.mem.Allocator, wait_ms: u64) !PollReport {
        try self.refuseIfRevoked();

        var changed_map: std.StringArrayHashMapUnmanaged(void) = .empty;
        var seeded_map: std.StringArrayHashMapUnmanaged(void) = .empty;

        // Schema first: a row in a new shape must find its table already moved.
        self.drainSchemaWatch(report_a, &changed_map, &seeded_map) catch |err| std.debug.print("schema watch: {s}\n", .{@errorName(err)});
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const streams = try self.cdcStreams(ca.allocator());
        if (streams.len == 0) return .{ .applied = 0, .settled = 0, .changed_tables = changed_map.keys(), .seeded = seeded_map.keys() };

        // ONE wait over every stream (nats.zig `PullInbox`, §10bh): one pull per
        // tail into a shared inbox, and the first message from any of them ends the
        // wait. This replaced a 250 ms round-robin whose cost was not the average
        // slice but a FIXED one: the write always lands on CDC_<tenant>, every poll
        // began with CDC_PUBLIC's full idle slice, and the row waited 265 ± 1 ms on
        // every one of 20 runs. Idle now costs one pull per stream per `wait_ms`.
        const a = ca.allocator();
        const subs = try a.alloc(*@import("nats").PullSubscription, streams.len);
        for (streams, 0..) |stream, i| subs[i] = (try self.tailFor(stream)).sub;
        const t: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(@intCast(@max(1, wait_ms))), .clock = .awake } };
        var applied: usize = 0;
        var mb = (try self.tailInbox()).fetch(subs, 100, t) catch |err| switch (err) {
            // EVERY tail's consumer is gone (a long pause past inactive_threshold):
            // re-open them all at the stored positions; the next poll reads.
            error.NoResponders => {
                for (self.tails.items) |*tl| try self.reopenTail(tl);
                return .{ .applied = 0, .settled = try self.drainVerdictsWith(0, 1), .changed_tables = changed_map.keys(), .seeded = seeded_map.keys() };
            },
            else => return err,
        };
        defer mb.deinit();
        // A consumer that went away while the others answered: re-open it alone.
        // By NAME, not by index — `gone` is ordered like `streams`, and the tails
        // list happens to match today only because states never change mid-life.
        for (mb.gone, 0..) |g, i| {
            if (g) try self.reopenTail(try self.tailFor(streams[i]));
        }
        // Messages come interleaved across streams; the applier positions per stream,
        // so group them (order within a stream is preserved).
        for (streams) |stream| {
            var mine: std.ArrayListUnmanaged(*@import("nats").JetStreamMessage) = .empty;
            for (mb.messages) |m| {
                if (std.mem.eql(u8, m.metadata.stream, stream)) try mine.append(a, m);
            }
            if (mine.items.len == 0) continue;
            const last = try self.storedSeq(stream);
            var max_seq = last;
            applied += try self.applyBatch(report_a, stream, mine.items, last, &max_seq, &changed_map);
        }
        if (applied > 0) self.retryHeld(report_a, &changed_map);
        self.drainRebase();
        const settled = try self.drainVerdictsWith(0, 1);
        self.heartbeatIfDue() catch |err| std.debug.print("heartbeat: {s}\n", .{@errorName(err)});
        return .{ .applied = applied, .settled = settled, .changed_tables = changed_map.keys(), .seeded = seeded_map.keys() };
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

        // A column this table does not have yet: the row is newer than the schema
        // (a migration whose descriptor has not reached us). Held — durably, in the
        // inbox — never dropped, never upserted into the wrong shape (the TS client's
        // unknown-column hold, CLIENTS.md divergence 2).
        if (!std.mem.eql(u8, op, "DELETE")) {
            var kit = data.object.iterator();
            while (kit.next()) |e| {
                const k = e.key_ptr.*;
                if (std.mem.startsWith(u8, k, "old.")) continue;
                const known = for (st.cols) |c| {
                    if (std.mem.eql(u8, c, k)) break true;
                } else false;
                if (!known) return error.SchemaBehind;
            }
        }

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
                // A parent's DELETE ahead of its children's (a cascade split across
                // batches) is HELD like a child ahead of its parent, and lands on retry.
                _ = self.stepExec(taa, stp) catch |err| {
                    if (err == storage.Error.StepFailed and std.mem.indexOf(u8, self.st.errMsg(), "FOREIGN KEY") != null) return error.FkHeld;
                    return err;
                };
            }
            // §10dg: whatever was held for this key (an INSERT waiting for its parent)
            // must not replay after the row's DELETE went by — it would resurrect it.
            pruneInboxKey(&self.st, taa, table, st.pk, data) catch {};
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
        // §10do: a row arriving may be the winner a held edit waits for.
        if (self.rebase.count() > 0) self.rebase_due = true;
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
        return self.mutateAt(result_a, table, op, key, values, null);
    }

    /// `mutate` with the caller's own stamp (the TypeScript client's `opts.version`):
    /// a host that keeps its own clock, or a test modelling a slow one. The HLC only
    /// advances: a stamp above it is followed, one below it is sent and forgotten.
    pub fn mutateAt(self: *SyncClient, result_a: std.mem.Allocator, table: []const u8, op: []const u8, key: Value, values: ?Value, stamp: ?[]const u8) ![]const u8 {
        var ca = std.heap.ArenaAllocator.init(self.a);
        defer ca.deinit();
        const a = ca.allocator();
        const st = self.states.get(table) orelse return error.UnknownTable;
        try self.ensureOutbox();

        const version = stamp orelse try core.hlcVersion(a, try nowWireIso(a), self.last_version, self.seen_floor);
        if (version.len > self.last_version_buf.len) return error.VersionTooLong;
        // The clock only moves forward: a caller's stamp from the past is sent as
        // given but never becomes what the next unstamped write follows.
        if (stamp == null or std.mem.order(u8, version, self.last_version) == .gt) {
            @memcpy(self.last_version_buf[0..version.len], version);
            self.last_version = self.last_version_buf[0..version.len];
        }

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

        // NON-FATAL, matching the TS client (its failed optimistic upsert is a log
        // line and the write still queues): the payload is valid ON THE WIRE — the
        // bridge builds the UPDATE from key + data — so a local echo that cannot
        // apply (an UPDATE whose values do not repeat the pk fails the upsert arm's
        // NOT NULL) must not abort the queueing. Aborting here LOST every such
        // update: 24 per client per swarm smoke, audible but wrong (§10cp). The
        // CDC echo applies the row properly moments later.
        // §10dx: the optimistic row carries the write's own stamp in the version column
        // (the ingress sets it from `version`); without it a NOT NULL version column
        // refused every optimistic INSERT locally.
        var opt = env.object.get("optimistic").?;
        if (st.version_col) |vc| {
            if (opt == .object and !std.mem.eql(u8, op, "DELETE")) {
                if (opt.object.get("data")) |d| {
                    if (d == .object and d.object.get(vc) == null) {
                        var stamped = d;
                        try stamped.object.put(a, vc, .{ .string = version });
                        try opt.object.put(a, "data", stamped);
                    }
                }
            }
        }
        self.applyOptimistic(table, st, opt) catch {};

        const payload_json = try core.valueToString(a, payload);
        const before_json: storage.Value = if (before) |b| .{ .text = try core.valueToString(a, b) } else .null;
        _ = try self.st.query(a,
            \\INSERT INTO _zebridge_outbox (msg_id, subject, payload, tbl, row_id, before, created_at, attempts)
            \\VALUES (?,?,?,?,?,?,?,0)
            \\ON CONFLICT(msg_id) DO UPDATE SET attempts = _zebridge_outbox.attempts + 1
        , &.{
            .{ .text = msg_id },         .{ .text = subject }, .{ .text = payload_json },
            .{ .text = table },          .{ .text = row_id },  before_json,
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
        const rows = try self.st.query(a, "SELECT msg_id, subject, payload, tbl, row_id, before FROM _zebridge_outbox ORDER BY created_at", &.{});
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
                    .{ if (r[3] == .text) r[3].text else "?", if (r[4] == .text) r[4].text else "?", msg_id, watermark orelse "?" },
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
        self.verdict_counts.count(status);
        if (!std.mem.eql(u8, status, "accepted")) {
            // Say it: a refusal that only the counters knew about was found by a run
            // whose every INSERT was rejected in silence (§10dx).
            const reason = if (v == .object) (if (v.object.get("reason")) |x| (if (x == .string) x.string else "") else "") else "";
            const detail = if (v == .object) (if (v.object.get("detail")) |x| (if (x == .string) x.string else "") else "") else "";
            std.debug.print("libzb: verdict {s} for {s}{s}{s}{s}{s}\n", .{ status, mid, if (reason.len > 0) " — " else "", reason, if (detail.len > 0) ": " else "", detail });
        }
        if (std.mem.eql(u8, status, "failed")) return false;
        if (std.mem.eql(u8, status, "rejected") or std.mem.eql(u8, status, "row_deleted")) {
            const rows = try self.st.query(a, "SELECT msg_id, subject, payload, tbl, row_id, before FROM _zebridge_outbox WHERE msg_id = ?", &.{.{ .text = mid }});
            // §10dy: `rejected` restores the before-image (nothing changed server-side);
            // `row_deleted` REMOVES the local row — the server's word is "deleted", and
            // restoring a before-image resurrected, on the emitter's own replica, a row
            // it had itself deleted a tick later (the TS client always deleted here).
            if (rows.len == 1) {
                if (std.mem.eql(u8, status, "row_deleted")) try self.removeOptimistic(a, rows[0]) else try self.revertOptimistic(a, rows[0]);
            }
        }
        // `stale` does not revert: the authoritative row is already on its way over
        // CDC, and restoring a before-image could undo a newer value. §10do: the edit
        // is kept aside instead — resubmitted if its columns are disjoint from the
        // winner's, dropped and surfaced otherwise (see `tryRebase`).
        if (std.mem.eql(u8, status, "stale")) {
            self.holdForRebase(a, mid) catch |err| std.debug.print("libzb: rebase hold for {s} failed: {any}\n", .{ mid, err });
        }
        _ = try self.st.query(a, "DELETE FROM _zebridge_outbox WHERE msg_id = ?", &.{.{ .text = mid }});
        return true;
    }

    // ─── §10do: rebase on stale ───────────────────────────────────────────────

    /// A `stale` verdict names an UPDATE that lost to a newer version. Its columns
    /// are not necessarily the columns the winner changed: when the two sets are
    /// disjoint the edit is still right and is resubmitted with a fresh stamp (the
    /// HLC floor puts it above the winner); when they overlap it is dropped and
    /// surfaced — LWW's word stands. Only an UPDATE qualifies; read BEFORE the
    /// outbox row goes.
    fn holdForRebase(self: *SyncClient, a: std.mem.Allocator, mid: []const u8) !void {
        const rows = try self.st.query(a, "SELECT subject, payload, tbl, before FROM _zebridge_outbox WHERE msg_id = ?", &.{.{ .text = mid }});
        if (rows.len != 1) return;
        const r = rows[0];
        if (r[0] != .text or r[1] != .text or r[2] != .text) return;
        const dot = std.mem.lastIndexOfScalar(u8, r[0].text, '.') orelse return;
        if (!std.mem.eql(u8, r[0].text[dot + 1 ..], "update")) return;
        const env = try parseStoredJson(a, r[1].text);
        if (env != .object) return;
        const key = env.object.get("key") orelse return;
        const data = env.object.get("data") orelse return;
        const ver = env.object.get("version") orelse return;
        if (key != .object or data != .object or ver != .string) return;
        const la = self.aa();
        try self.rebase.put(la, try la.dupe(u8, mid), .{
            .table = try la.dupe(u8, r[2].text),
            .key = try la.dupe(u8, try core.valueToString(a, key)),
            .values = try la.dupe(u8, try core.valueToString(a, data)),
            .before = if (r[3] == .text) try la.dupe(u8, r[3].text) else null,
            .version = try la.dupe(u8, try core.normalizeVersion(a, ver.string)),
        });
        self.rebase_due = true;
    }

    /// Runs at the end of poll() and flush(), outside any apply transaction: the
    /// resubmit is a full mutate() (outbox + optimistic apply), sent by the host's
    /// next flush like any other write.
    fn drainRebase(self: *SyncClient) void {
        if (!self.rebase_due or self.rebase.count() == 0) return;
        self.rebase_due = false;
        var ta = std.heap.ArenaAllocator.init(self.a);
        defer ta.deinit();
        var i: usize = 0;
        while (i < self.rebase.count()) {
            const mid = self.rebase.keys()[i];
            const done = self.tryRebase(ta.allocator(), mid) catch |err| blk: {
                std.debug.print("libzb: rebase of {s} deferred: {any}\n", .{ mid, err });
                self.rebase_due = true;
                break :blk false;
            };
            if (done) _ = self.rebase.swapRemoveAt(i) else i += 1;
        }
    }

    /// True once the entry is settled (rebased or dropped); false while the winning
    /// row is not here yet — it is, when its version is above the refused stamp.
    fn tryRebase(self: *SyncClient, a: std.mem.Allocator, mid: []const u8) !bool {
        const e = self.rebase.get(mid) orelse return true;
        const st = self.states.get(e.table) orelse return true;
        const vcol = st.version_col orelse return true;
        const key = try parseStoredJson(a, e.key);
        const cur = (try self.beforeImage(a, e.table, st, key)) orelse {
            std.debug.print("libzb: rebase of {s} abandoned: the row is gone\n", .{mid});
            return true;
        };
        if (cur != .object) return true;
        const cur_ver = if (cur.object.get(vcol)) |v| (if (v == .string) try core.normalizeVersion(a, v.string) else "") else "";
        if (std.mem.order(u8, cur_ver, e.version) != .gt) return false;
        const values = try parseStoredJson(a, e.values);
        const before: ?Value = if (e.before) |b| try parseStoredJson(a, b) else null;
        var mine: std.ArrayList(u8) = .empty;
        var overlap: std.ArrayList(u8) = .empty;
        for (values.object.keys()) |c| {
            if (std.mem.eql(u8, c, vcol)) continue;
            if (mine.items.len > 0) try mine.appendSlice(a, ", ");
            try mine.appendSlice(a, c);
            // The winner changed c when the row here holds neither the pre-write value
            // nor mine: the CDC row overwrote my optimistic copy with something else.
            const winner_changed = if (before) |b|
                (b != .object or (!sameJson(a, cur.object.get(c), b.object.get(c)) and !sameJson(a, cur.object.get(c), values.object.get(c))))
            else
                true;
            if (winner_changed) {
                if (overlap.items.len > 0) try overlap.appendSlice(a, ", ");
                try overlap.appendSlice(a, c);
            }
        }
        if (overlap.items.len > 0) {
            std.debug.print("libzb: {s} edit LOST to a newer version on the same column(s) {s} — the winning row stands; surface this to the user\n", .{ e.table, overlap.items });
            return true;
        }
        const ver = try self.mutate(a, e.table, "UPDATE", key, values);
        std.debug.print("libzb: rebased {s} of {s} onto the newer row as {s}\n", .{ mine.items, e.table, ver });
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
    ///
    /// The GAP follows the budget, capped at 250 ms (§10ef). It was a fixed 250 ms,
    /// so `flush(0)` — the loop's "send what is queued, take what is there" turn —
    /// blocked a quarter second after the last verdict, every turn: a 5 ms sting
    /// ran at four ticks a second, and the Flutter worker's turn cost 350 ms against
    /// the 100 ms the README promised. A short budget now means a short gap; what a
    /// 1 ms gap leaves behind, the next poll's sweep takes.
    pub fn drainVerdicts(self: *SyncClient, wait_ms: u64) !usize {
        return self.drainVerdictsWith(wait_ms, @min(250, @max(1, wait_ms)));
    }

    /// `per_msg_ms` is the wait for EACH next message; `poll` passes 1 so a sweep with
    /// nothing pending costs a millisecond, not the 250 ms a settle wait affords.
    fn drainVerdictsWith(self: *SyncClient, wait_ms: u64, per_msg_ms: u64) !usize {
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
                .{ .duration = .{ .raw = .fromMilliseconds(@intCast(per_msg_ms)), .clock = .awake } },
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
            if (std.mem.eql(u8, mid, "revoked")) return self.hangUpRevoked();

            // ⚠️ JSON, not msgpack. CDC and mutations are msgpack; a verdict is JSON —
            // it is read by humans and by `nats sub` as often as by a client. The wire
            // names are the bridge's: accepted / stale / row_deleted / rejected settle
            // the entry (the last two revert the optimistic copy); `failed` stays queued.
            if (try self.settleVerdict(a, mid, msg.data)) settled += 1;
        }
        return settled;
    }

    /// §10dm: the ban, acted on — the socket dropped now, the flag set for every later
    /// call. Cooperative: this library obeys; the JWT revocation is the enforcement.
    fn hangUpRevoked(self: *SyncClient) error{Revoked} {
        self.revoked = true;
        std.debug.print("{s}: REVOKED by the operator (mutation_ack.{s}.revoked) — hanging up now; every later call answers Revoked. The local rows stay; the wipe is the application's (zb_client_wipe).\n", .{ self.opts.principal, self.opts.principal });
        // The socket is dropped at the NEXT entry point, not here: this runs inside the
        // verdict drain, over a subscription the close would free under the loop's feet
        // (measured: SIGSEGV in the host right after the ban).
        return error.Revoked;
    }

    /// Every entry point: once revoked, drop the socket (once) and answer Revoked.
    fn refuseIfRevoked(self: *SyncClient) !void {
        if (!self.revoked) return;
        if (!self.hung_up) {
            self.hung_up = true;
            self.t.conn.close();
        }
        return error.Revoked;
    }

    /// At connect: a ban published while this client was away is retained on the
    /// MUTATIONS stream — one direct get, before anything is read.
    fn probeRevoked(self: *SyncClient) !void {
        var buf: [256]u8 = undefined;
        const subject = try std.fmt.bufPrint(&buf, "{s}.{s}.revoked", .{ self.subject_mutation_ack_prefix, self.opts.principal });
        if (self.t.lastBySubject(self.stream_mutations, subject) catch null) |m| {
            m.deinit();
            return self.hangUpRevoked();
        }
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
    /// The `row_deleted` revert: the row is gone upstream, so it goes here too.
    fn removeOptimistic(self: *SyncClient, a: std.mem.Allocator, r: storage.Row) !void {
        const table = if (r[3] == .text) r[3].text else return;
        const st = self.states.get(table) orelse return;
        const env = if (r[2] == .text) try parseStoredJson(a, r[2].text) else return;
        const key = if (env == .object) env.object.get("key") orelse return else return;
        if (try core.planDelete(a, table, st.pk, key)) |stp| _ = self.stepExec(a, stp) catch {};
    }

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

    /// Fleet observability (NOTES §10dc): once per `heartbeat_ms`, publish this client's
    /// applied position per CDC stream to `$KV.<live>.<tenant>.<principal>` — last value
    /// per key, TTL on the bucket, so a client that stops beating simply goes stale. The
    /// bridge reads the bucket on its own cadence and turns head − applied into lag.
    /// Cooperative: a failed beat is printed and retried on the next turn, never fatal.
    fn heartbeatIfDue(self: *SyncClient) !void {
        if (self.opts.heartbeat_ms == 0 or self.tenant.len == 0) return;
        const now = nowMillis();
        if (self.last_heartbeat_ms != 0 and now - self.last_heartbeat_ms < @as(i64, @intCast(self.opts.heartbeat_ms))) return;
        var ha = std.heap.ArenaAllocator.init(self.a);
        defer ha.deinit();
        const a = ha.allocator();
        const streams = try self.cdcStreams(a);
        const seqs = try a.alloc(u64, streams.len);
        for (streams, 0..) |s, i| seqs[i] = try self.storedSeq(s);
        const payload = try core.heartbeatPayload(a, self.opts.principal, self.tenant, now, streams, seqs);
        const subject = try std.fmt.allocPrint(a, "$KV.{s}.{s}.{s}", .{ self.kv_live, self.tenant, self.opts.principal });
        try self.t.publish(subject, payload, null);
        self.last_heartbeat_ms = now;
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

    // ── the index card (NOTES §10): query / mutate / sync / flush ────────────────

    /// The app's read: arbitrary SQL against the read-only connection, JSON params in,
    /// `{"columns":[…],"rows":[[…],…]}` out — allocated from `a`. A write through here
    /// fails in SQLite ("attempt to write a readonly database"), which is the point.
    pub fn query(self: *SyncClient, a: std.mem.Allocator, sql: []const u8, params_json: Value) !Value {
        const n: usize = if (params_json == .array) params_json.array.items.len else 0;
        const params = try a.alloc(storage.Value, n);
        if (params_json == .array) for (params_json.array.items, 0..) |v, i| {
            params[i] = try jsonToStorage(a, v);
        };
        const res = try self.ro.queryNamed(a, sql, params);
        var cols = std.json.Array.init(a);
        for (res.columns) |cn| try cols.append(.{ .string = cn });
        var rows = std.json.Array.init(a);
        for (res.rows) |r| {
            var row = std.json.Array.init(a);
            for (r) |cell| try row.append(storageToJson(cell));
            try rows.append(.{ .array = row });
        }
        var out: std.json.ObjectMap = .empty;
        try out.put(a, "columns", .{ .array = cols });
        try out.put(a, "rows", .{ .array = rows });
        return .{ .object = out };
    }

    pub const SyncReport = struct { tenant: []const u8, first: bool };

    /// One pass of the read side: the one-time steps (tenant, schemas, outbox table,
    /// the verdict channel) the first time, then seed-if-gapped and drain-to-tail every
    /// time. Idempotent by construction — a host calls it at boot and on a timer.
    pub fn syncOnce(self: *SyncClient) !SyncReport {
        try self.refuseIfRevoked();
        const first = !self.schemas_synced;
        if (first) {
            try self.resolveTenant();
            try self.probeRevoked();
            try self.ensureOutbox();
            try self.subscribeVerdicts();
            self.schemas_synced = true;
        }
        // Every pass, not only the first: `migrateTable` decides from the database and
        // is a no-op on an identical descriptor, so a schema change published while
        // this client runs lands on its next sync (§10v's alter/rebuild path).
        try self.syncSchemas();
        try self.gapAndSeed(null, null);
        try self.drainCdc();
        // A host that syncs before it ever polls is a client too (PROTOCOL §9).
        self.heartbeatIfDue() catch |err| std.debug.print("heartbeat: {s}\n", .{@errorName(err)});
        return .{ .tenant = self.tenant, .first = first };
    }

    pub const FlushReport = struct { sent: usize, settled: usize };

    pub const VerdictCounts = struct {
        accepted: usize = 0,
        stale: usize = 0,
        rejected: usize = 0,
        row_deleted: usize = 0,
        failed: usize = 0,
        other: usize = 0,
        fn count(self: *VerdictCounts, status: []const u8) void {
            if (std.mem.eql(u8, status, "accepted")) self.accepted += 1 else if (std.mem.eql(u8, status, "stale")) self.stale += 1 else if (std.mem.eql(u8, status, "rejected")) self.rejected += 1 else if (std.mem.eql(u8, status, "row_deleted")) self.row_deleted += 1 else if (std.mem.eql(u8, status, "failed")) self.failed += 1 else self.other += 1;
        }
    };

    /// The write side's pump: collect and replay the outbox (§7.1), then wait up to
    /// `wait_ms` for the first verdict and drain what follows.
    pub fn flush(self: *SyncClient, wait_ms: u64) !FlushReport {
        try self.refuseIfRevoked();
        const sent = try self.flushOutbox();
        const settled = try self.drainVerdicts(wait_ms);
        self.drainRebase();
        return .{ .sent = sent, .settled = settled };
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
    // Not under test: the grammar test provokes this on purpose, and the build
    // runner reports any stderr from a test step as a failed command.
    if (!@import("builtin").is_test) {
        std.debug.print("grammar.json: required key missing or not a string: ", .{});
        for (path, 0..) |k, i| std.debug.print("{s}{s}", .{ if (i > 0) "." else "", k });
        std.debug.print("\n", .{});
    }
    return error.GrammarKeyMissing;
}

/// `_zbz_inbox` — held child-before-parent events, durable (CLIENTS.md, §10de finding 1).
/// Same shape as the TS client's `_zebridge_inbox`; standalone on `Storage` so a test
/// needs no NATS and no client.
/// §10dg: the shape this replica BUILT each table with (core.keyShape/typeShape) —
/// the record a re-key or a re-type is detected against.
pub fn ensureShape(st: *storage.Storage) !void {
    try st.execSimple("CREATE TABLE IF NOT EXISTS _zbz_shape (tbl TEXT PRIMARY KEY, key_shape TEXT NOT NULL, type_shape TEXT NOT NULL)");
}

pub fn ensureInbox(st: *storage.Storage) !void {
    try st.execSimple("CREATE TABLE IF NOT EXISTS _zbz_inbox (id INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT NOT NULL, lsn INTEGER NOT NULL, ev TEXT NOT NULL, reason TEXT NOT NULL, held_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0)");
    try st.execSimple("CREATE INDEX IF NOT EXISTS _zbz_inbox_tbl ON _zbz_inbox (tbl, lsn)");
}

/// Drop every held event of `table` whose key equals the deleted row's (§10dg).
pub fn pruneInboxKey(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, pk: []const []const u8, data: Value) !void {
    if (pk.len == 0 or data != .object) return;
    const rows = try st.query(a, "SELECT id, ev FROM _zbz_inbox WHERE tbl = ?", &.{.{ .text = table }});
    for (rows) |r| {
        const ev = std.json.parseFromSliceLeaky(Value, a, r[1].text, .{}) catch continue;
        const d = if (ev == .object) (ev.object.get("data") orelse continue) else continue;
        if (d != .object) continue;
        var same = true;
        for (pk) |col| {
            const x = data.object.get(col) orelse {
                same = false;
                break;
            };
            const y = d.object.get(col) orelse {
                same = false;
                break;
            };
            if (!valueEql(x, y)) {
                same = false;
                break;
            }
        }
        if (same) {
            _ = try st.query(a, "DELETE FROM _zbz_inbox WHERE id = ?", &.{r[0]});
            std.debug.print("{s}: a held event for a row deleted upstream was discarded\n", .{table});
        }
    }
}

fn valueEql(x: Value, y: Value) bool {
    return switch (x) {
        .string => |s| y == .string and std.mem.eql(u8, s, y.string),
        .integer => |i| y == .integer and y.integer == i,
        .bool => |b| y == .bool and y.bool == b,
        else => false,
    };
}

pub fn holdEvent(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, ev: Value, reason: []const u8) !void {
    const lsn: i64 = if (ev == .object) (if (ev.object.get("lsn")) |v| (if (v == .integer) v.integer else 0) else 0) else 0;
    const json = try core.valueToString(a, ev);
    _ = try st.query(a, "INSERT INTO _zbz_inbox (tbl, lsn, ev, reason, held_at) VALUES (?, ?, ?, ?, ?)", &.{
        .{ .text = table }, .{ .integer = lsn }, .{ .text = json }, .{ .text = reason }, .{ .integer = nowMillis() },
    });
}

pub fn pruneInboxSeeded(st: *storage.Storage, a: std.mem.Allocator, table: []const u8, watermark_lsn: i64) !void {
    _ = try st.query(a, "DELETE FROM _zbz_inbox WHERE tbl = ? AND lsn <= ?", &.{ .{ .text = table }, .{ .integer = watermark_lsn } });
}

pub fn pruneInboxDropped(st: *storage.Storage, a: std.mem.Allocator, table: []const u8) !void {
    const q = try st.query(a, "SELECT count(*) FROM _zbz_inbox WHERE tbl = ?", &.{.{ .text = table }});
    const k: i64 = if (q.len > 0 and q[0][0] == .integer) q[0][0].integer else 0;
    if (k > 0) std.debug.print("{s}: discarding {d} held event(s) — the table was dropped upstream\n", .{ table, k });
    _ = try st.query(a, "DELETE FROM _zbz_inbox WHERE tbl = ?", &.{.{ .text = table }});
}

test "inbox: a held event survives closing the database, and a seed past it prunes it" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa_ = arena.allocator();
    const path = "/tmp/zb-inbox-test.sqlite3";
    for ([_][]const u8{ path, path ++ "-wal", path ++ "-shm" }) |f| std.Io.Dir.cwd().deleteFile(std.testing.io, f) catch {};
    const ev = (try std.json.parseFromSlice(Value, aa_,
        \\{"table":"child","operation":"INSERT","lsn":10,"data":{"uid":"c1","parent_id":"p1"}}
    , .{})).value;
    {
        var st = try storage.Storage.open(path);
        defer st.close();
        try ensureInbox(&st);
        try holdEvent(&st, aa_, "child", ev, "missing-parent");
    }
    // The host died here. A fresh open still holds the row.
    var st = try storage.Storage.open(path);
    defer st.close();
    try ensureInbox(&st);
    var rows = try st.query(aa_, "SELECT tbl, lsn, reason FROM _zbz_inbox", &.{});
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("child", rows[0][0].text);
    try std.testing.expectEqual(@as(i64, 10), rows[0][1].integer);
    // A chain seeded short of it leaves it; one seeded past it supersedes it.
    try pruneInboxSeeded(&st, aa_, "child", 9);
    rows = try st.query(aa_, "SELECT id FROM _zbz_inbox", &.{});
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try pruneInboxSeeded(&st, aa_, "child", 10);
    rows = try st.query(aa_, "SELECT id FROM _zbz_inbox", &.{});
    try std.testing.expectEqual(@as(usize, 0), rows.len);
    // Dropped upstream: discarded.
    try holdEvent(&st, aa_, "child", ev, "missing-parent");
    try pruneInboxDropped(&st, aa_, "child");
    rows = try st.query(aa_, "SELECT id FROM _zbz_inbox", &.{});
    try std.testing.expectEqual(@as(usize, 0), rows.len);
}

test "grammar: a missing key fails naming its path, never a silent default" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ok = try std.json.parseFromSlice(Value, a,
        \\{"kv":{"schemas":"schemas"}}
    , .{});
    try std.testing.expectEqualStrings("schemas", try grammarString(ok.value, &.{ "kv", "schemas" }));
    try std.testing.expectError(error.GrammarKeyMissing, grammarString(ok.value, &.{ "kv", "tenants" }));
    try std.testing.expectError(error.GrammarKeyMissing, grammarString(ok.value, &.{"open_tenant"}));
    const empty = try std.json.parseFromSlice(Value, a,
        \\{"open_tenant":""}
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

fn sameStrings(x: []const []const u8, y: []const []const u8) bool {
    if (x.len != y.len) return false;
    for (x, y) |p, q| if (!std.mem.eql(u8, p, q)) return false;
    return true;
}

fn dupeStrings(a: std.mem.Allocator, src: []const []const u8) ![]const []const u8 {
    const out = try a.alloc([]const u8, src.len);
    for (src, 0..) |sv, i| out[i] = try a.dupe(u8, sv);
    return out;
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
/// §10do: a stale UPDATE kept aside for a rebase. `before` is the pre-write image
/// the outbox stored; null when the row was not here at write time (then every
/// column counts as changed by the winner, and the edit is dropped).
const Rebase = struct { table: []const u8, key: []const u8, values: []const u8, before: ?[]const u8, version: []const u8 };

fn sameJson(a: std.mem.Allocator, x: ?Value, y: ?Value) bool {
    const xs = core.valueToString(a, x orelse .null) catch return false;
    const ys = core.valueToString(a, y orelse .null) catch return false;
    return std.mem.eql(u8, xs, ys);
}

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
/// arena it was decoded into (an FK-held event, before the inbox made holds durable).
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
        yd.year,              md.month.numeric(),      @as(u32, md.day_index) + 1,
        ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute(),
        micros,
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
    const src = try std.json.parseFromSlice(Value, a,
        \\{"key":{"uid":"u1"},"version":"2026-01-01T00:00:00.000000Z","n":[1,2,3]}
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
        .db_path = bad,
        .principal = "t",
        .tables = &.{},
    });
    try std.testing.expectError(storage.Error.OpenFailed, r);
}

test "init with another protocol's grammar hash refuses, and leaks nothing — including the open database" {
    // The database DOES open here, so this is the case that used to leave a live
    // SQLite handle behind: errdefer destroyed the struct and closed nothing.
    const r = SyncClient.init(std.testing.allocator, .{
        .url = "nats://127.0.0.1:1",
        .creds_path = "/nonexistent.creds",
        .grammar_hash = "0000000000000000000000000000000000000000000000000000000000000000",
        .db_path = "zbz-test-ownership.sqlite3",
        .principal = "t",
        .tables = &.{},
    });
    try std.testing.expectError(error.GrammarMismatch, r);
    _ = std.c.unlink("zbz-test-ownership.sqlite3");
}

test "the embedded grammar parses and its hash is the bridge's form" {
    const g = try std.json.parseFromSlice(Value, std.testing.allocator, grammar_json, .{});
    defer g.deinit();
    try std.testing.expect(g.value == .object);
    try std.testing.expect(g.value.object.get("streams") != null);
    var buf: [64]u8 = undefined;
    const h = grammarHashHex(&buf);
    try std.testing.expectEqual(@as(usize, 64), h.len);
    for (h) |c| try std.testing.expect(std.ascii.isHex(c));
}

test "migrateTable: create, ALTER add/remove, rename hint, FK change rebuilds — the row survives each" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa_ = arena.allocator();
    const path = "/tmp/zb-migrate-test.sqlite3";
    for ([_][]const u8{ path, path ++ "-wal", path ++ "-shm" }) |f| std.Io.Dir.cwd().deleteFile(std.testing.io, f) catch {};
    var st = try storage.Storage.open(path);
    defer st.close();

    const desc = struct {
        fn make(al: std.mem.Allocator, json: []const u8) !Value {
            return (try std.json.parseFromSlice(Value, al, json, .{})).value;
        }
    };
    // 1. first sight: created (with an index)
    const d1 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"a","type":"INTEGER"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[{"name":"t_a","columns":["a"]}]}
    );
    try std.testing.expectEqual(SyncClient.Migration.created, try SyncClient.migrateTable(&st, aa_, "t", d1));
    _ = try st.query(aa_, "INSERT INTO t (uid, a, updated_at) VALUES ('u1', 7, 'v1')", &.{});
    try std.testing.expectEqual(SyncClient.Migration.unchanged, try SyncClient.migrateTable(&st, aa_, "t", d1));

    // 2. add b, remove a — ALTER; the index on a goes with it; the view exists
    const d2 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"b","type":"TEXT"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[]}
    );
    try std.testing.expectEqual(SyncClient.Migration.altered, try SyncClient.migrateTable(&st, aa_, "t", d2));
    var rows = try st.query(aa_, "SELECT uid, b FROM t", &.{});
    try std.testing.expectEqual(1, rows.len);
    try std.testing.expectEqualStrings("u1", rows[0][0].text);
    try std.testing.expectEqual(0, (try st.query(aa_, "SELECT name FROM sqlite_master WHERE type='index' AND name='t_a'", &.{})).len);
    try std.testing.expectEqual(1, (try st.query(aa_, "SELECT name FROM sqlite_master WHERE type='view' AND name='t_view'", &.{})).len);

    // 3. a rename hint: b → c keeps the value (§1.2: without the hint it would be lost)
    _ = try st.query(aa_, "UPDATE t SET b = 'kept'", &.{});
    const d3 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"c","type":"TEXT"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[],"renamed":{"c":"b"}}
    );
    try std.testing.expectEqual(SyncClient.Migration.altered, try SyncClient.migrateTable(&st, aa_, "t", d3));
    rows = try st.query(aa_, "SELECT c FROM t", &.{});
    try std.testing.expectEqualStrings("kept", rows[0][0].text);

    // 4. an FK appears: SQLite cannot ALTER a constraint in — rebuild, row copied
    _ = try st.query(aa_, "CREATE TABLE p (uid TEXT PRIMARY KEY)", &.{});
    _ = try st.query(aa_, "INSERT INTO p VALUES ('u1')", &.{});
    const d4 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"c","type":"TEXT"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[{"columns":["uid"],"references":"p","parent_columns":["uid"]}],"indexes":[{"name":"t_c","columns":["c"]}]}
    );
    try std.testing.expectEqual(SyncClient.Migration.rebuilt, try SyncClient.migrateTable(&st, aa_, "t", d4));
    rows = try st.query(aa_, "SELECT c FROM t", &.{});
    try std.testing.expectEqual(1, rows.len);
    try std.testing.expectEqualStrings("kept", rows[0][0].text);
    try std.testing.expect(std.mem.indexOf(u8, (try st.query(aa_, "SELECT sql FROM sqlite_master WHERE name='t'", &.{}))[0][0].text, "FOREIGN KEY") != null);
    try std.testing.expectEqual(1, (try st.query(aa_, "SELECT name FROM sqlite_master WHERE type='index' AND name='t_c'", &.{})).len);
    try std.testing.expectEqual(SyncClient.Migration.unchanged, try SyncClient.migrateTable(&st, aa_, "t", d4));
    // and the FK is live again after the surgery
    try std.testing.expectError(error.StepFailed, st.query(aa_, "INSERT INTO t (uid, c, updated_at) VALUES ('orphan', 'x', 'v')", &.{}));

    // 5. §10dg re-type: c TEXT → REAL, same names — invisible to diffColumns, caught by
    // the shape record; SQLite has no ALTER COLUMN TYPE, so a rebuild that keeps the row.
    const d5 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"TEXT"},{"name":"c","type":"REAL"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[{"columns":["uid"],"references":"p","parent_columns":["uid"]}],"indexes":[{"name":"t_c","columns":["c"]}]}
    );
    try std.testing.expectEqual(SyncClient.Migration.rebuilt, try SyncClient.migrateTable(&st, aa_, "t", d5));
    rows = try st.query(aa_, "SELECT c FROM t", &.{});
    try std.testing.expectEqual(1, rows.len);
    try std.testing.expect(std.mem.indexOf(u8, (try st.query(aa_, "SELECT sql FROM sqlite_master WHERE name='t'", &.{}))[0][0].text, "\"c\" REAL") != null);
    try std.testing.expectEqual(SyncClient.Migration.unchanged, try SyncClient.migrateTable(&st, aa_, "t", d5));

    // 6. §10dg re-key: the pk uid TEXT → INTEGER (the bigserial→uuid shape, mirrored).
    // Same names again — but a key cannot move in place: rebuilt EMPTY, the row is gone,
    // and the shape record now names the new key.
    const d6 = try desc.make(aa_,
        \\{"pk_columns":["uid"],"sqlite":{"columns":[{"name":"uid","type":"INTEGER"},{"name":"c","type":"REAL"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[]}
    );
    try std.testing.expectEqual(SyncClient.Migration.rekeyed, try SyncClient.migrateTable(&st, aa_, "t", d6));
    try std.testing.expectEqual(0, (try st.query(aa_, "SELECT uid FROM t", &.{})).len);
    try std.testing.expect(std.mem.indexOf(u8, (try st.query(aa_, "SELECT sql FROM sqlite_master WHERE name='t'", &.{}))[0][0].text, "\"uid\" INTEGER NOT NULL PRIMARY KEY") != null);
    try std.testing.expectEqualStrings("[[\"uid\",\"INTEGER\"]]", (try st.query(aa_, "SELECT key_shape FROM _zbz_shape WHERE tbl='t'", &.{}))[0][0].text);
    try std.testing.expectEqual(1, (try st.query(aa_, "SELECT name FROM sqlite_master WHERE type='view' AND name='t_view'", &.{})).len);
    try std.testing.expectEqual(SyncClient.Migration.unchanged, try SyncClient.migrateTable(&st, aa_, "t", d6));
    // 7. a composite key (uid, c) — the pk column SET moved: re-key again
    const d7 = try desc.make(aa_,
        \\{"pk_columns":["uid","c"],"sqlite":{"columns":[{"name":"uid","type":"INTEGER"},{"name":"c","type":"REAL"},{"name":"updated_at","type":"TEXT"}]},
        \\ "foreign_keys":[],"indexes":[]}
    );
    try std.testing.expectEqual(SyncClient.Migration.rekeyed, try SyncClient.migrateTable(&st, aa_, "t", d7));
    try std.testing.expect(std.mem.indexOf(u8, (try st.query(aa_, "SELECT sql FROM sqlite_master WHERE name='t'", &.{}))[0][0].text, "PRIMARY KEY (\"uid\", \"c\")") != null);
}

test "migrateTable: a suspension descriptor is an error, never a panic" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const path = "/tmp/zb-migrate-susp.sqlite3";
    for ([_][]const u8{ path, path ++ "-wal", path ++ "-shm" }) |f| std.Io.Dir.cwd().deleteFile(std.testing.io, f) catch {};
    var st = try storage.Storage.open(path);
    defer st.close();
    const susp = (try std.json.parseFromSlice(Value, arena.allocator(), "{\"table\":\"t\",\"suspended\":\"no_cdc_subject\",\"lsn\":1}", .{})).value;
    try std.testing.expectError(error.SchemaUnusable, SyncClient.migrateTable(&st, arena.allocator(), "t", susp));
    const junk = (try std.json.parseFromSlice(Value, arena.allocator(), "[1,2]", .{})).value;
    try std.testing.expectError(error.SchemaUnusable, SyncClient.migrateTable(&st, arena.allocator(), "t", junk));
}
