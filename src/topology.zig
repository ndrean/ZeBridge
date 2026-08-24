//! Wire names, read at **startup** from topology.json.
//!
//! These used to be compile-time constants: `build.zig` `@embedFile`d topology.json and
//! `addOptions` baked every stream, subject and bucket name into the binary. That bought
//! a real guarantee — a key missing from the file failed the *build*, which is what
//! stopped the bridge and `nats-init` drifting apart — and it cost more than it was
//! worth. `nats-init` reads the same file at container run time with `jq`, so a rename
//! reached the server on `docker compose up` while the bridge kept the names it was
//! compiled with, and logged them as though it had just read them. Renaming a stream was
//! a rebuild and a redeploy, not a restart.
//!
//! The guarantee is not lost, only moved: a missing or malformed key now fails at
//! **startup**, before a single thread exists, with the key named. That is the same place
//! `DATABASE_URL` is enforced, and an operator error belongs there rather than in a
//! compiler.
//!
//! One file, three readers: the bridge (here), `nats-init` (jq, creating the streams),
//! and clients (`web-consumer` imports it directly). That is the whole point of it.

const std = @import("std");

pub const log = std.log.scoped(.topology);

/// Where topology.json is, when `TOPOLOGY_PATH` does not say.
pub const default_path = "topology.json";

pub const Error = error{
    MissingKey,
    NotAnObject,
    NotAString,
};

/// Where a parse or render failed, in words.
///
/// Zig errors carry no payload, and "MissingKey" without the key is a worse message than
/// none. `load` fills one of these and logs it; `parse` and `render` stay pure so a test
/// can exercise every failure path without the test runner treating their own diagnostics
/// as failures.
pub const Diagnostic = struct {
    /// e.g. `subjects`, or the pattern being rendered.
    context: []const u8 = "",
    /// e.g. `snapshot_data_pattern`, or the offending placeholder.
    detail: []const u8 = "",

    fn set(self: ?*Diagnostic, context: []const u8, detail: []const u8) void {
        if (self) |d| d.* = .{ .context = context, .detail = detail };
    }
};

/// Every name the bridge puts on the wire. Slices are owned by the `Owned` that produced
/// them and live as long as the process.
pub const Topology = struct {
    // ─── streams ────────────────────────────────────────────────────────────────
    stream_cdc: []const u8,
    stream_init: []const u8,
    stream_mutations: []const u8,
    stream_requests: []const u8,

    // ─── per-tenant CDC streams (empty when the deployment is not tenant-routed) ──
    //
    // The tenant list is what the boot preflight checks NATS against: every name here
    // must have a `<cdc_stream_prefix><TENANT>` stream, or the bridge refuses to start.
    // A row whose tenant is not in this list has no destination — the whole reason the
    // check exists (NOTES.md §1.12, §4.5).
    tenants: []const []const u8,
    cdc_stream_prefix: []const u8,
    cdc_stream_public: []const u8,

    /// Tables `CDC_PUBLIC`'s subject filter actually covers when they are *not*
    /// tenant-scoped — `nats-init`/`up.sh` build the stream's subjects from exactly this
    /// list. A table that is neither tenant-scoped (in `TENANT_RULES`) nor listed here
    /// has no destination for `cdc.<table>.<op>` at all — see `isCdcRoutable`.
    public_tables: []const []const u8,

    // ─── per-tenant INIT streams (same reasoning as CDC above) ────────────────────
    //
    // A JetStream consumer's `filter_subject` is reader-chosen and unchecked by any
    // subject permission — the stream name is the actual enforcement unit for a pull
    // consumer. One wide INIT stream with tenant only in the subject would let any
    // client with consumer-creation rights on it choose a different tenant's
    // `filter_subject` and read those chunks — the exact leak `crosstenant.py` proved
    // for CDC before that split (NOTES.md §1.12).
    init_stream_prefix: []const u8,
    init_stream_public: []const u8,

    // ─── subject prefixes ───────────────────────────────────────────────────────
    subject_cdc_prefix: []const u8,
    subject_init_prefix: []const u8,
    subject_mutations_prefix: []const u8,

    // ─── ingress ────────────────────────────────────────────────────────────────
    mutation_pattern: []const u8,
    mutation_error_prefix: []const u8,
    mutation_error_pattern: []const u8,
    /// Where a per-write verdict is published: `mutation_ack.<principal>.<msg_id>`.
    ///
    /// Keyed by the client's own `Nats-Msg-Id` rather than by table, because the client
    /// needs to match a verdict to *the entry in its queue*, and by principal because
    /// that makes `mutation_ack.alice.>` authorizable with the same allow-list that
    /// already governs `mutation.alice.>`.
    mutation_ack_prefix: []const u8,
    mutation_ack_pattern: []const u8,

    // ─── requests ───────────────────────────────────────────────────────────────
    snapshot_request: []const u8,
    /// `snapshot.request.{[tenant]s}.{[table]s}` — tenant-keyed, not principal-keyed:
    /// a dump is shared by every principal in a tenant, and principal-keying would
    /// make a fleet restart spawn one serialized snapshot per principal per table
    /// (NOTES.md §1.12 part 2).
    snapshot_request_pattern: []const u8,

    // ─── snapshot subjects ──────────────────────────────────────────────────────
    snapshot_data_pattern: []const u8,
    snapshot_start_pattern: []const u8,
    snapshot_error_pattern: []const u8,
    snapshot_meta_pattern: []const u8,
    snapshot_schema_pattern: []const u8,

    // ─── KV buckets ─────────────────────────────────────────────────────────────
    kv_schemas: []const u8,
    kv_snapshots: []const u8,
    kv_tenants: []const u8,

    // ─── generations (NOTES.md §1.13) ───────────────────────────────────────────
    /// KV bucket holding the chain manifests, keyed `<tenant>.<table>` — tenant first,
    /// which is what makes per-tenant `DIRECT.GET` grants scopable at all.
    kv_generations: []const u8,
    /// Object-store bucket per tenant: `<prefix><tenant>`. A bucket IS a stream
    /// (`OBJ_<bucket>`), so the prefix is part of the ACL boundary, same as `CDC_`.
    generation_bucket_prefix: []const u8,

    // ─── derived, built once at load ────────────────────────────────────────────
    //
    // These were `++` concatenations when the names were comptime. Same values, computed
    // at startup instead — the alternative is rebuilding them at every call site, which
    // is how a "$KV.schemas.{s}" literal ended up hardcoded in four places before.

    /// Every mutation subject the ingress consumer filters on: `mutation.>`
    mutations_subject_wildcard: []const u8,
    /// JetStream exposes a KV bucket as `$KV.<bucket>.<key>`.
    kv_schemas_subject_pattern: []const u8,
    /// `$KV.snapshots.{[tenant]s}.{[table]s}` — tenant-keyed like the request and data
    /// subjects above, and for the same reason: keyed by table alone, a second
    /// requester's descriptor would overwrite the first's across tenants (NOTES.md
    /// §1.12 part 2).
    kv_snapshots_subject_pattern: []const u8,
    /// `$KV.tenants.{[principal]s}` — PROTOCOL.md "The Connection Flow", Step 0.
    kv_tenants_subject_pattern: []const u8,
    /// `snapshot.request.` — a request's subject minus the table.
    request_subject_prefix: []const u8,
    /// `snapshot.request.>` — what the snapshot consumer filters on.
    request_subject_wildcard: []const u8,

    /// Whether `cdc.<table>.<op>` (untenanted) or `cdc.<tenant>.<table>.<op>` (any
    /// tenant) has *some* stream willing to accept it — not whether the table replicates
    /// correctly in every other respect, just whether the publish has anywhere to go.
    ///
    /// Tenant-scoped tables are always covered: `CDC_<TENANT>` exists for every declared
    /// tenant (preflight's own boot check enforces that), and `CDC_PUBLIC` additionally
    /// covers the open-tenant case — so `TENANT_RULES` membership alone is sufficient,
    /// with no need to special-case which tenant a row carries. `is_tenant_scoped` is the
    /// caller's own `tenant_rules.contains(table)`, not re-derived here, because
    /// `TransitionRules` lives in `config.zig`, which already imports this module —
    /// importing it back would be circular.
    ///
    /// An untenanted table is covered only if it is in `public_tables`, exactly the list
    /// `nats-init`/`up.sh` used to build `CDC_PUBLIC`'s subjects — anything else was
    /// never added to any stream's subject filter, by construction, not by a bug.
    pub fn isCdcRoutable(self: *const Topology, table: []const u8, is_tenant_scoped: bool) bool {
        if (is_tenant_scoped) return true;
        for (self.public_tables) |t| {
            if (std.mem.eql(u8, t, table)) return true;
        }
        return false;
    }

    /// A fixed set of names for tests, so a unit test never needs a file on disk.
    /// **Not a fallback**: nothing in the running bridge reads this, because a default
    /// that silently substitutes for a missing file is exactly the drift this module
    /// exists to prevent.
    pub const for_tests = Topology{
        .stream_cdc = "CDC",
        .stream_init = "INIT",
        .stream_mutations = "MUTATIONS",
        .stream_requests = "REQUESTS",
        .tenants = &.{},
        .public_tables = &.{},
        .cdc_stream_prefix = "CDC_",
        .cdc_stream_public = "CDC_PUBLIC",
        .init_stream_prefix = "INIT_",
        .init_stream_public = "INIT_PUBLIC",
        .subject_cdc_prefix = "cdc",
        .subject_init_prefix = "init",
        .subject_mutations_prefix = "mutation",
        .mutation_pattern = "mutation.{[principal]s}.{[table]s}.{[operation]s}",
        .mutation_error_prefix = "mutation_error",
        .mutation_error_pattern = "mutation_error.{[table]s}",
        .mutation_ack_prefix = "mutation_ack",
        .mutation_ack_pattern = "mutation_ack.{[principal]s}.{[msg_id]s}",
        .snapshot_request = "snapshot.request",
        .snapshot_request_pattern = "snapshot.request.{[tenant]s}.{[table]s}",
        .snapshot_data_pattern = "init.snap.{[tenant]s}.{[table]s}.{[snapshot_id]s}.{[chunk]d}",
        .snapshot_start_pattern = "init.snap.{[tenant]s}.start.{[table]s}",
        .snapshot_error_pattern = "init.snap.{[tenant]s}.error.{[table]s}",
        .snapshot_meta_pattern = "init.snap.{[tenant]s}.meta.{[table]s}",
        .snapshot_schema_pattern = "init.snap.{[tenant]s}.schema.{[table]s}.{[snapshot_id]s}",
        .kv_schemas = "schemas",
        .kv_snapshots = "snapshots",
        .kv_tenants = "tenants",
        .kv_generations = "generations",
        .generation_bucket_prefix = "gen-",
        .mutations_subject_wildcard = "mutation.>",
        .kv_schemas_subject_pattern = "$KV.schemas.{[table]s}",
        .kv_snapshots_subject_pattern = "$KV.snapshots.{[tenant]s}.{[table]s}",
        .kv_tenants_subject_pattern = "$KV.tenants.{[principal]s}",
        .request_subject_prefix = "snapshot.request.",
        .request_subject_wildcard = "snapshot.request.>",
    };
};

/// A parsed topology plus the arena backing every string in it.
pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    topology: Topology,

    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
    }
};

/// Read and parse `path`. Every string in the result is copied into the returned arena,
/// so the file's bytes are not retained.
pub fn load(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Owned {
    // 64 KiB is absurdly generous for a name table and still bounds a mistake.
    const bytes = std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(64 * 1024)) catch |err| {
        log.err(
            "🔴 cannot read topology '{s}': {s}. It carries every stream and subject name the bridge, nats-init and the clients share; there is no built-in default, because one would silently disagree with the file nats-init read. Set TOPOLOGY_PATH, or run from the directory holding it.",
            .{ path, @errorName(err) },
        );
        return err;
    };
    defer allocator.free(bytes);

    var diag: Diagnostic = .{};
    return parse(allocator, bytes, &diag) catch |err| {
        log.err("🔴 topology '{s}': {s} at \"{s}\".\"{s}\"", .{ path, @errorName(err), diag.context, diag.detail });
        return err;
    };
}

/// Parse topology JSON. Separated from `load` so the whole thing is testable without a
/// filesystem.
pub fn parse(allocator: std.mem.Allocator, bytes: []const u8, diag: ?*Diagnostic) !Owned {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();

    var parsed = std.json.parseFromSlice(std.json.Value, allocator, bytes, .{}) catch |err| {
        Diagnostic.set(diag, "(document)", "not valid JSON");
        return err;
    };
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |o| o,
        else => {
            Diagnostic.set(diag, "(document)", "must be a JSON object");
            return Error.NotAnObject;
        },
    };

    const streams = try section(root, "streams", diag);
    const subjects = try section(root, "subjects", diag);
    const kv = try section(root, "kv", diag);

    var t: Topology = undefined;

    t.stream_cdc = try str(a, streams, "streams", "cdc", diag);
    t.stream_init = try str(a, streams, "streams", "init", diag);
    t.stream_mutations = try str(a, streams, "streams", "mutations", diag);
    t.stream_requests = try str(a, streams, "streams", "requests", diag);

    // Optional: a deployment with no `tenants` is not tenant-routed, and that is a valid
    // shape (the single wide CDC stream). Absent means an empty list, never an error.
    t.tenants = try optStrArray(a, root, "tenants");
    // Same optionality: absent means no untenanted table is CDC-routable, which is a
    // real (if unusual) shape rather than a mistake to reject.
    t.public_tables = try optStrArray(a, root, "public_tables");
    if (root.get("cdc_streams")) |cs| switch (cs) {
        .object => |o| {
            t.cdc_stream_prefix = if (o.get("tenant_prefix")) |v| (switch (v) {
                .string => |x| try a.dupe(u8, x),
                else => "CDC_",
            }) else "CDC_";
            t.cdc_stream_public = if (o.get("public")) |v| (switch (v) {
                .string => |x| try a.dupe(u8, x),
                else => "CDC_PUBLIC",
            }) else "CDC_PUBLIC";
        },
        else => {
            t.cdc_stream_prefix = "CDC_";
            t.cdc_stream_public = "CDC_PUBLIC";
        },
    } else {
        t.cdc_stream_prefix = "CDC_";
        t.cdc_stream_public = "CDC_PUBLIC";
    }

    // Same optionality as cdc_streams, same reasoning: absent means not tenant-routed.
    if (root.get("init_streams")) |is| switch (is) {
        .object => |o| {
            t.init_stream_prefix = if (o.get("tenant_prefix")) |v| (switch (v) {
                .string => |x| try a.dupe(u8, x),
                else => "INIT_",
            }) else "INIT_";
            t.init_stream_public = if (o.get("public")) |v| (switch (v) {
                .string => |x| try a.dupe(u8, x),
                else => "INIT_PUBLIC",
            }) else "INIT_PUBLIC";
        },
        else => {
            t.init_stream_prefix = "INIT_";
            t.init_stream_public = "INIT_PUBLIC";
        },
    } else {
        t.init_stream_prefix = "INIT_";
        t.init_stream_public = "INIT_PUBLIC";
    }

    t.subject_cdc_prefix = try str(a, subjects, "subjects", "cdc_prefix", diag);
    t.subject_init_prefix = try str(a, subjects, "subjects", "init_prefix", diag);
    t.subject_mutations_prefix = try str(a, subjects, "subjects", "mutations_prefix", diag);

    t.mutation_pattern = try str(a, subjects, "subjects", "mutation_pattern", diag);
    t.mutation_error_prefix = try str(a, subjects, "subjects", "mutation_error_prefix", diag);
    t.mutation_error_pattern = try str(a, subjects, "subjects", "mutation_error_pattern", diag);
    t.mutation_ack_prefix = try str(a, subjects, "subjects", "mutation_ack_prefix", diag);
    t.mutation_ack_pattern = try str(a, subjects, "subjects", "mutation_ack_pattern", diag);

    t.snapshot_request = try str(a, subjects, "subjects", "snapshot_request", diag);
    t.snapshot_request_pattern = try str(a, subjects, "subjects", "snapshot_request_pattern", diag);

    t.snapshot_data_pattern = try str(a, subjects, "subjects", "snapshot_data_pattern", diag);
    t.snapshot_start_pattern = try str(a, subjects, "subjects", "snapshot_start_pattern", diag);
    t.snapshot_error_pattern = try str(a, subjects, "subjects", "snapshot_error_pattern", diag);
    t.snapshot_meta_pattern = try str(a, subjects, "subjects", "snapshot_meta_pattern", diag);
    t.snapshot_schema_pattern = try str(a, subjects, "subjects", "snapshot_schema_pattern", diag);

    t.kv_schemas = try str(a, kv, "kv", "schemas", diag);
    t.kv_snapshots = try str(a, kv, "kv", "snapshots", diag);
    t.kv_tenants = try str(a, kv, "kv", "tenants", diag);

    // Optional, like cdc_streams: a deployment that never sets GENERATION_RULES has no
    // reason to name these, and absent means the defaults the feature was built against.
    if (root.get("generations")) |gv| switch (gv) {
        .object => |o| {
            t.kv_generations = if (o.get("kv")) |v| (switch (v) {
                .string => |x| try a.dupe(u8, x),
                else => "generations",
            }) else "generations";
            t.generation_bucket_prefix = if (o.get("bucket_prefix")) |v| (switch (v) {
                .string => |x| try a.dupe(u8, x),
                else => "gen-",
            }) else "gen-";
        },
        else => {
            t.kv_generations = "generations";
            t.generation_bucket_prefix = "gen-";
        },
    } else {
        t.kv_generations = "generations";
        t.generation_bucket_prefix = "gen-";
    }

    // Derived. `$KV.` is not in topology.json on purpose: it is JetStream's own subject
    // mapping for a bucket, not a name anyone is free to choose.
    t.mutations_subject_wildcard = try std.fmt.allocPrint(a, "{s}.>", .{t.subject_mutations_prefix});
    t.kv_schemas_subject_pattern = try std.fmt.allocPrint(a, "$KV.{s}.{{[table]s}}", .{t.kv_schemas});
    t.kv_snapshots_subject_pattern = try std.fmt.allocPrint(a, "$KV.{s}.{{[tenant]s}}.{{[table]s}}", .{t.kv_snapshots});
    t.kv_tenants_subject_pattern = try std.fmt.allocPrint(a, "$KV.{s}.{{[principal]s}}", .{t.kv_tenants});
    t.request_subject_prefix = try std.fmt.allocPrint(a, "{s}.", .{t.snapshot_request});
    t.request_subject_wildcard = try std.fmt.allocPrint(a, "{s}.>", .{t.snapshot_request});

    return .{ .arena = arena, .topology = t };
}

/// An optional array of strings. Absent, wrong-typed, or containing a non-string element
/// all yield an **empty** slice rather than an error: these fields are optional by design,
/// and a deployment that omits `tenants` is choosing the untenanted shape, not making a
/// mistake. Every element is duped into the arena.
fn optStrArray(a: std.mem.Allocator, root: std.json.ObjectMap, key: []const u8) ![]const []const u8 {
    const v = root.get(key) orelse return &.{};
    const arr = switch (v) {
        .array => |x| x,
        else => return &.{},
    };
    var out: std.ArrayListUnmanaged([]const u8) = .empty;
    for (arr.items) |item| switch (item) {
        .string => |sv| try out.append(a, try a.dupe(u8, sv)),
        else => {},
    };
    return try out.toOwnedSlice(a);
}

fn section(root: std.json.ObjectMap, name: []const u8, diag: ?*Diagnostic) !std.json.ObjectMap {
    const v = root.get(name) orelse {
        Diagnostic.set(diag, name, "(whole section missing)");
        return Error.MissingKey;
    };
    return switch (v) {
        .object => |o| o,
        else => {
            Diagnostic.set(diag, name, "must be an object");
            return Error.NotAnObject;
        },
    };
}

/// Read one key, naming both the section and the key when it is absent — the message is
/// the whole reason this is not `orelse return error.MissingKey`.
fn str(
    a: std.mem.Allocator,
    obj: std.json.ObjectMap,
    section_name: []const u8,
    key: []const u8,
    diag: ?*Diagnostic,
) ![]const u8 {
    const v = obj.get(key) orelse {
        Diagnostic.set(diag, section_name, key);
        return Error.MissingKey;
    };
    return switch (v) {
        .string => |s| try a.dupe(u8, s),
        else => {
            Diagnostic.set(diag, section_name, key);
            return Error.NotAString;
        },
    };
}

// ─── rendering ──────────────────────────────────────────────────────────────────

pub const RenderError = error{
    UnknownPlaceholder,
    UnusedArgument,
    MalformedPattern,
    OutOfMemory,
};

pub const Arg = struct {
    name: []const u8,
    value: []const u8,
};

/// Substitute `{[name]s}` / `{[name]d}` placeholders in a pattern.
///
/// `std.fmt.allocPrint` needs a **comptime** format string, so the moment these patterns
/// came from a file at runtime it stopped being usable for them. The syntax is kept
/// exactly as it was so topology.json does not change: a half-upgraded deployment cannot
/// end up publishing to a differently-shaped subject.
///
/// Both directions are checked. An unknown placeholder means the file asks for something
/// the call site cannot supply; an unused argument means the call site knows a name the
/// pattern no longer uses. Either way the subject would be wrong, and a wrong subject is
/// silent — the message is stored under a name nobody is listening on, or under none at
/// all if no stream captures it.
pub fn render(
    allocator: std.mem.Allocator,
    pattern: []const u8,
    args: []const Arg,
    diag: ?*Diagnostic,
) RenderError![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);

    // One bit per argument; every one must be consumed.
    var used = [_]bool{false} ** 8;
    if (args.len > used.len) return RenderError.MalformedPattern;

    var i: usize = 0;
    while (i < pattern.len) {
        const c = pattern[i];

        // `}}` is an escaped closing brace, again as in std.fmt. A lone `}` is passed
        // through: it cannot appear in a NATS subject anyway, and rejecting it would turn
        // a harmless character into a startup failure.
        if (c == '}' and i + 1 < pattern.len and pattern[i + 1] == '}') {
            try out.append(allocator, '}');
            i += 2;
            continue;
        }

        if (c != '{') {
            try out.append(allocator, c);
            i += 1;
            continue;
        }

        // `{{` is a literal brace, as in std.fmt.
        if (i + 1 < pattern.len and pattern[i + 1] == '{') {
            try out.append(allocator, '{');
            i += 2;
            continue;
        }

        // Expect `{[name]x}`.
        if (i + 1 >= pattern.len or pattern[i + 1] != '[') return RenderError.MalformedPattern;
        const name_start = i + 2;
        const name_end = std.mem.indexOfScalarPos(u8, pattern, name_start, ']') orelse
            return RenderError.MalformedPattern;
        const close = std.mem.indexOfScalarPos(u8, pattern, name_end, '}') orelse
            return RenderError.MalformedPattern;

        const name = pattern[name_start..name_end];
        var found = false;
        for (args, 0..) |arg, idx| {
            if (std.mem.eql(u8, arg.name, name)) {
                try out.appendSlice(allocator, arg.value);
                used[idx] = true;
                found = true;
                break;
            }
        }
        if (!found) {
            Diagnostic.set(diag, pattern, name);
            return RenderError.UnknownPlaceholder;
        }

        i = close + 1;
    }

    for (args, 0..) |arg, idx| {
        if (!used[idx]) {
            Diagnostic.set(diag, pattern, arg.name);
            return RenderError.UnusedArgument;
        }
    }

    return out.toOwnedSlice(allocator);
}

// ─── tests ──────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "render: substitutes named placeholders in order" {
    const out = try render(testing.allocator, "init.snap.{[table]s}.{[snapshot_id]s}.{[chunk]d}", &.{
        .{ .name = "table", .value = "users" },
        .{ .name = "snapshot_id", .value = "snap-1-ab" },
        .{ .name = "chunk", .value = "3" },
    }, null);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings("init.snap.users.snap-1-ab.3", out);
}

test "render: the type letter is ignored — everything arrives as text" {
    // `{[chunk]d}` is a Zig format spelling; at runtime the caller has already turned the
    // number into digits, and honouring `d` would mean re-parsing it to print it back.
    const out = try render(testing.allocator, "a.{[n]d}.b", &.{.{ .name = "n", .value = "42" }}, null);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings("a.42.b", out);
}

test "render: a placeholder the call site cannot supply is an error, not an empty token" {
    // Silently substituting "" would publish to `init.snap..snap-1` — a subject that
    // still matches `init.>`, so the stream accepts it and no one ever notices.
    try testing.expectError(
        RenderError.UnknownPlaceholder,
        render(testing.allocator, "init.snap.{[tabel]s}", &.{.{ .name = "table", .value = "users" }}, null),
    );
}

test "render: an argument the pattern never uses is an error too" {
    // The other direction: topology.json dropped a placeholder, so the value silently
    // stops appearing in the subject and every table shares one.
    try testing.expectError(
        RenderError.UnusedArgument,
        render(testing.allocator, "init.snap.{[table]s}", &.{
            .{ .name = "table", .value = "users" },
            .{ .name = "snapshot_id", .value = "snap-1" },
        }, null),
    );
}

test "render: literal braces survive" {
    const out = try render(testing.allocator, "{{literal}} {[t]s}", &.{.{ .name = "t", .value = "x" }}, null);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings("{literal} x", out);
}

test "render: malformed patterns are rejected rather than half-substituted" {
    try testing.expectError(
        RenderError.MalformedPattern,
        render(testing.allocator, "a.{table}", &.{.{ .name = "table", .value = "x" }}, null),
    );
    try testing.expectError(
        RenderError.MalformedPattern,
        render(testing.allocator, "a.{[table}", &.{.{ .name = "table", .value = "x" }}, null),
    );
}

test "parse: reads every name and derives the composites" {
    const json =
        \\{
        \\  "streams": {"cdc":"C","init":"I","mutations":"M","requests":"R"},
        \\  "subjects": {
        \\    "cdc_prefix":"cdc","init_prefix":"init","mutations_prefix":"mut",
        \\    "mutation_pattern":"mut.{[principal]s}.{[table]s}.{[operation]s}",
        \\    "mutation_error_prefix":"mut_err","mutation_error_pattern":"mut_err.{[table]s}",
        \\    "mutation_ack_prefix":"mut_ack","mutation_ack_pattern":"mut_ack.{[principal]s}.{[msg_id]s}",
        \\    "snapshot_request":"snap.req","snapshot_request_pattern":"snap.req.{[tenant]s}.{[table]s}",
        \\    "snapshot_data_pattern":"d","snapshot_start_pattern":"s",
        \\    "snapshot_error_pattern":"e","snapshot_meta_pattern":"m",
        \\    "snapshot_schema_pattern":"sc"
        \\  },
        \\  "kv": {"schemas":"sch","snapshots":"snp","tenants":"ten"}
        \\}
    ;
    var owned = try parse(testing.allocator, json, null);
    defer owned.deinit();
    const t = owned.topology;

    try testing.expectEqualStrings("C", t.stream_cdc);
    try testing.expectEqualStrings("mut", t.subject_mutations_prefix);

    // Derived from the parsed values, not from literals.
    try testing.expectEqualStrings("mut.>", t.mutations_subject_wildcard);
    try testing.expectEqualStrings("snap.req.", t.request_subject_prefix);
    try testing.expectEqualStrings("snap.req.>", t.request_subject_wildcard);
    try testing.expectEqualStrings("$KV.sch.{[table]s}", t.kv_schemas_subject_pattern);
    try testing.expectEqualStrings("$KV.snp.{[tenant]s}.{[table]s}", t.kv_snapshots_subject_pattern);
    try testing.expectEqualStrings("$KV.ten.{[principal]s}", t.kv_tenants_subject_pattern);
}

test "parse: a missing key fails at load, naming it" {
    // This is the guarantee the compile-time version had. It moved; it did not go.
    const json =
        \\{"streams": {"cdc":"C","init":"I","mutations":"M"},
        \\ "subjects": {}, "kv": {}}
    ;
    try testing.expectError(Error.MissingKey, parse(testing.allocator, json, null));
}

test "parse: a missing section fails at load" {
    try testing.expectError(Error.MissingKey, parse(testing.allocator, "{\"streams\":{}}", null));
}

test "parse: a non-string name is rejected" {
    const json =
        \\{"streams": {"cdc":42,"init":"I","mutations":"M","requests":"R"},
        \\ "subjects": {}, "kv": {}}
    ;
    try testing.expectError(Error.NotAString, parse(testing.allocator, json, null));
}

test "the test fixture matches the repository's own topology.json" {
    // `Topology.for_tests` is a convenience, and a convenience that drifts from the real
    // file is worse than none: unit tests would pass against names the bridge never uses.
    var threaded: std.Io.Threaded = .init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const bytes = std.Io.Dir.cwd().readFileAlloc(io, "topology.json", testing.allocator, .limited(64 * 1024)) catch |err| {
        // `zig build test` runs from the repository root; if that ever stops being true,
        // skip rather than fail on a path.
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);

    var owned = try parse(testing.allocator, bytes, null);
    defer owned.deinit();
    const real = owned.topology;
    const fixture = Topology.for_tests;

    inline for (@typeInfo(Topology).@"struct".fields) |f| {
        // `tenants` is the one field that is not a name or a pattern — it is data
        // (whichever tenants a deployment happens to declare), so it is `[]const
        // []const u8`, not `[]const u8`, and `expectEqualStrings` does not even
        // typecheck against it. It is also deliberately excluded from "matches the
        // repository's own topology.json" on purpose: `for_tests` carries no tenants
        // so unit tests default to untenanted behaviour, while the real file declares
        // some for the multi-tenant scenarios — that difference is intentional, not
        // drift, and this test exists to catch drift in the *names*, not to assert the
        // fixture is a byte-for-byte copy of the file.
        if (comptime std.mem.eql(u8, f.name, "tenants")) continue;
        // Same reasoning as `tenants` — a data field, not a name, and `for_tests`
        // deliberately carries none so unit tests default to no untenanted table being
        // CDC-routable via `public_tables` either.
        if (comptime std.mem.eql(u8, f.name, "public_tables")) continue;
        try testing.expectEqualStrings(@field(fixture, f.name), @field(real, f.name));
    }
}
