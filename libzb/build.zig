const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // System SQLite: the storage shell links it directly (macOS: the Homebrew
    // keg; Linux: the distro package). Same translate-c pattern the bridge
    // uses for libpq.
    const default_sqlite: []const u8 = if (builtin.os.tag == .macos)
        "/opt/homebrew/opt/sqlite"
    else
        "/usr";
    const sqlite_prefix = b.option([]const u8, "sqlite-prefix", "System SQLite prefix") orelse default_sqlite;

    // §10iq: VENDORED sqlite and zstd, compiled from pinned sources by Zig itself
    // instead of linked from the host. Two reasons, one of them measured the hard way:
    //
    //   * a PHONE cannot use the host's copies. Neither the iOS SDK nor the Android NDK
    //     ships zstd at all, and linking the system sqlite on Android is discouraged —
    //     which is as far as §10ig got before running out of libraries to find.
    //   * the host's copies are a silent coupling. A plain `zig build` once produced a
    //     dylib with no DuckDB in it and said nothing (§10hw); the same shape of
    //     surprise applies to a Homebrew upgrade moving sqlite under a running project.
    //
    // Opt-in for now, so nothing that works today changes: `-Dvendor=true`.
    const vendor = b.option(bool, "vendor", "Compile sqlite and zstd from pinned sources instead of linking the host's") orelse false;

    const translate_c = b.addTranslateC(.{
        .root_source_file = b.path("src/sqlite_includes.h"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    // libzstd for the chain objects: streaming inflate of frames that state no
    // content size (§10gi), at native speed — std.compress.zstd was 4.2 s for a
    // 102 MB full (§10ez).
    const zstd_prefix: []const u8 = if (builtin.os.tag == .macos) "/opt/homebrew/opt/zstd" else "/usr";
    // §10iq: the HEADERS come from the pinned sources when vendoring, so translate-c
    // reads the same sqlite3.h and zstd.h that will actually be compiled in.
    const sqlite_dep = if (vendor) b.dependency("sqlite", .{}) else null;
    const zstd_dep = if (vendor) b.dependency("zstd", .{}) else null;
    if (vendor) {
        translate_c.addIncludePath(sqlite_dep.?.path("."));
        translate_c.addIncludePath(zstd_dep.?.path("lib"));
    } else {
        translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "include" }) });
        translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "include" }) });
    }
    // libpq for the PostgreSQL replica engine (§10fd): a client's storage may be a
    // PostgreSQL server instead of a SQLite file — the micro-VM case.
    //
    // §10jk: the engines' headers are vendored (include-engines/), so every build —
    // phones included — compiles the DuckDB and PostgreSQL engines without either
    // library installed. Neither is LINKED: engines.zig opens them at run time.
    translate_c.addIncludePath(b.path("include-engines"));
    // §10jk: and a CROSS target's libc headers: libpq-fe.h includes stdio.h, which the
    // phone SDKs hold under the sysroot — the same reach `addVendored` gives the C
    // sources (§10iq), Android's per-architecture directory included (§10ir).
    const sysroot_includes: []const []const u8 = if (b.sysroot) |sr| blk: {
        const base = b.pathJoin(&.{ sr, "usr", "include" });
        if (target.result.abi == .android or target.result.abi == .androideabi) {
            const triple = target.result.linuxTriple(b.allocator) catch break :blk &.{base};
            break :blk b.allocator.dupe([]const u8, &.{ base, b.pathJoin(&.{ base, triple }) }) catch &.{base};
        }
        break :blk b.allocator.dupe([]const u8, &.{base}) catch &.{};
    } else &.{};
    for (sysroot_includes) |p| translate_c.addSystemIncludePath(.{ .cwd_relative = p });
    const c_mod = translate_c.createModule();

    // §10fl: DuckDB's header as its own module (`@import("duckdb")`), from the vendored
    // copy. The library itself is opened at run time (§10jk, engines.zig).
    const duckdb_mod: *std.Build.Module = blk: {
        const tc = b.addTranslateC(.{
            .root_source_file = b.path("src/duckdb_includes.h"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        tc.addIncludePath(b.path("include-engines"));
        for (sysroot_includes) |p| tc.addSystemIncludePath(.{ .cwd_relative = p });
        break :blk tc.createModule();
    };

    // §10iq: the vendored C, added to whichever module links it. One function so the
    // four modules cannot drift — that drift is exactly what let a plain `zig build`
    // silently drop DuckDB (§10hw).
    //
    // zstd's own build is a set of directories, not a single file: `common` is shared,
    // `compress` and `decompress` are what libzb calls into. `dictBuilder` is NOT here:
    // nothing trains a dictionary any more (§10iy), and nothing loads one.
    // §10iq: zstd's sources, NAMED rather than globbed — a build should not depend on
    // what happens to be in a directory, and iterating one at configure time needs a
    // filesystem handle this Zig version does not hand out here.
    //
    // `dictBuilder` is deliberately absent: no dictionary is trained or loaded anywhere
    // since 2026-09-24 (§10iy).
    const android_api = b.option([]const u8, "android-api", "Android API level whose libc a shared build links (NDK sysroot)") orelse "29";
    const arch_include: ?[]const u8 = if (target.result.abi == .android or target.result.abi == .androideabi)
        target.result.linuxTriple(b.allocator) catch null
    else
        null;

    const zstd_srcs = [_][]const u8{
        "lib/common/debug.c",         "lib/common/entropy_common.c",
        "lib/common/error_private.c", "lib/common/fse_decompress.c",
        "lib/common/pool.c",          "lib/common/threading.c",
        "lib/common/xxhash.c",        "lib/common/zstd_common.c",
        "lib/compress/fse_compress.c","lib/compress/hist.c",
        "lib/compress/huf_compress.c","lib/compress/zstd_compress.c",
        "lib/compress/zstd_compress_literals.c",
        "lib/compress/zstd_compress_sequences.c",
        "lib/compress/zstd_compress_superblock.c",
        "lib/compress/zstd_double_fast.c",
        "lib/compress/zstd_fast.c",   "lib/compress/zstd_lazy.c",
        "lib/compress/zstd_ldm.c",    "lib/compress/zstd_opt.c",
        "lib/compress/zstd_preSplit.c",
        "lib/compress/zstdmt_compress.c",
        "lib/decompress/huf_decompress.c",
        "lib/decompress/zstd_ddict.c",
        "lib/decompress/zstd_decompress.c",
        "lib/decompress/zstd_decompress_block.c",
    };

    // The vendored C, added to whichever module links it. One function so the four
    // modules cannot drift — that drift is exactly what let a plain `zig build`
    // silently drop DuckDB (§10hw).
    const addVendored = struct {
        fn f(
            bb: *std.Build,
            m: *std.Build.Module,
            sq: ?*std.Build.Dependency,
            zs: ?*std.Build.Dependency,
            srcs: []const []const u8,
            arch_inc: ?[]const u8,
            api: []const u8,
        ) void {
            const sqd = sq orelse return;
            const zsd = zs.?;
            // ⚠️ §10iq: C sources need a libc to compile against, and a CROSS target
            // has none until it is told. `--sysroot $(xcrun --sdk iphonesimulator
            // --show-sdk-path)` is how the iOS SDK arrives; without this line the
            // vendored zstd fails on `'string.h' file not found` even though the
            // sysroot was given, because that flag reaches the Zig compilation and not
            // these C files.
            if (bb.sysroot) |sr| {
                m.addSystemIncludePath(.{ .cwd_relative = bb.pathJoin(&.{ sr, "usr", "include" }) });
                // ⚠️ Android's NDK sysroot splits its headers: the portable ones live in
                // usr/include, the architecture's own (asm/types.h and friends) in
                // usr/include/<triple>. Without this, sqlite and zstd find stdio.h and
                // then fail on 'asm/types.h file not found'.
                if (arch_inc) |ai| {
                    m.addSystemIncludePath(.{ .cwd_relative = bb.pathJoin(&.{ sr, "usr", "include", ai }) });
                    // ⚠️ And Android's libc to LINK a shared object against, which lives
                    // under an API-LEVEL directory: usr/lib/<triple>/<api>. Without it a
                    // dynamic build stops at "unable to provide libc for target
                    // aarch64-linux-android"; a static one never needed it.
                    // `-Dandroid-api` chooses the level (21 is the NDK's oldest).
                    // RELATIVE: Zig prepends the sysroot itself (the same trap as the iOS path).
                    m.addLibraryPath(.{ .cwd_relative = bb.pathJoin(&.{ "/usr", "lib", ai, api }) });
                }
                // …and the libc to LINK against. Headers alone get as far as
                // "unable to find libSystem system library".
                //
                // ⚠️ RELATIVE to the sysroot, which Zig prepends itself: passing the
                // absolute path produced `<sdk>/<sdk>/usr/lib` and found nothing.
                m.addLibraryPath(.{ .cwd_relative = "/usr/lib" });
            }
            m.addIncludePath(sqd.path("."));
            m.addCSourceFile(.{
                .file = sqd.path("sqlite3.c"),
                // The amalgamation compiles bare by default; these are what libzb's
                // storage actually relies on.
                .flags = &.{
                    "-DSQLITE_ENABLE_COLUMN_METADATA=1",
                    "-DSQLITE_THREADSAFE=1",
                    "-DSQLITE_DQS=0",
                    "-DSQLITE_OMIT_LOAD_EXTENSION=1",
                    "-DSQLITE_USE_ALLOCA=1",
                },
            });
            m.addIncludePath(zsd.path("lib"));
            m.addIncludePath(zsd.path("lib/common"));
            for (srcs) |src| {
                m.addCSourceFile(.{
                    .file = zsd.path(src),
                    // ⚠️ No assembly: zstd's .S file is x86-64 only and would break
                    // every arm64 target, which is every target that matters here.
                    .flags = &.{ "-DZSTD_DISABLE_ASM=1", "-DXXH_NAMESPACE=ZSTD_" },
                });
            }
            m.link_libc = true;
        }
    }.f;

    const nats_dep = b.dependency("nats", .{ .target = target, .optimize = optimize });
    const msgpack_dep = b.dependency("zig_msgpack", .{ .target = target, .optimize = optimize });

    const mod = b.createModule(.{
        .root_source_file = b.path("src/capi.zig"),
        // §10ir: Android links the archive into a shared library with the NDK's clang
        // (Zig cannot synthesise bionic), and that needs position-independent objects —
        // the vendored C included: "relocation R_AARCH64_ABS64 cannot be used against
        // local symbol; recompile with -fPIC" (2026-09-24).
        .pic = if (target.result.abi == .android or target.result.abi == .androideabi) true else null,
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("c", c_mod);
    // §10dq: the wire grammar is compiled in — the same bytes the bridge embeds.
    mod.addAnonymousImport("grammar", .{ .root_source_file = b.path("../src/grammar.json") });
    mod.addImport("nats", nats_dep.module("nats"));
    mod.addImport("msgpack", msgpack_dep.module("msgpack"));
    // §10iq: vendored, or the host's — never both.
    if (vendor) {
        addVendored(b, mod, sqlite_dep, zstd_dep, &zstd_srcs, arch_include, android_api);
    } else {
        mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
        mod.linkSystemLibrary("sqlite3", .{});
        mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
        mod.linkSystemLibrary("zstd", .{});
    }
    mod.link_libc = true;
    mod.addImport("duckdb", duckdb_mod);

    // The C-ABI shared library: one JSON dispatch entrypoint (zb_call) +
    // zb_free. Hosts: Python (ctypes), and later Dart/Swift/Kotlin/.NET FFI.
    // §10iq: STATIC when cross-compiling, dynamic otherwise. A phone links libzb INTO
    // the app — iOS ships static archives or frameworks, not loose dylibs — and a
    // shared library additionally pulls Zig's stack-trace machinery, which wants
    // `__dyld_get_image_header_containing_address`: a symbol the iOS simulator SDK does
    // not export. The desktop hosts (Python ctypes, Dart ffi) need the dynamic one, and
    // a native build still gets it.
    // §10ir: an ARCHIVE or a shared library. The default follows the platform's own
    // convention rather than a blanket rule for cross builds:
    //
    //   * iOS links archives or frameworks INTO the app, and a shared build additionally
    //     drags Zig's stack-trace machinery, which wants
    //     `__dyld_get_image_header_containing_address` — not exported by the simulator SDK.
    //   * ANDROID ships `.so` files under jniLibs, so dynamic looks like the native
    //     shape — but Zig cannot synthesise Android's libc for a shared build and stops
    //     at "unable to provide libc for target aarch64-linux-android". A library path
    //     is not enough; that needs a `--libc` paths file describing the NDK. The static
    //     archive builds today and is the normal way in anyway: the app's own JNI shim
    //     is the `.so`, and libzb links INTO it.
    //   * a desktop host (Python ctypes, dart:ffi) loads a dylib.
    //
    // ⚠️ An archive is ~5.6x the shared library for identical code (22.6 MB against
    // 4.06 MB) and that is NOT its cost: object files keep every symbol and relocation,
    // nothing is dead-stripped, and the linker pulls only the members an app references.
    // Compare linked artefacts, never an archive against a shared library.
    const default_static = target.result.os.tag == .ios or
        target.result.abi == .android or target.result.abi == .androideabi;
    // iOS and Android carry Mozilla's CA bundle (src/roots.zig): say when it has aged.
    if (default_static) warnStaleRoots(b);
    const static = b.option(bool, "static", "Build a static archive instead of a shared library") orelse default_static;
    const lib = b.addLibrary(.{
        .name = "zbcore",
        .root_module = mod,
        .linkage = if (static) .static else .dynamic,
    });
    // §10iy: a static library another linker consumes must carry compiler-rt itself —
    // Xcode's ld found `roundq` (f128, compiler-rt's) undefined in the force-loaded
    // archive on the first iOS link.
    lib.bundle_compiler_rt = true;
    b.installArtifact(lib);

    // §10iq: `zig build lib` — the LIBRARY on its own. The default step also builds
    // `zb`, `zb-demo` and `zb-soak`, which are developer tools that make no sense on a
    // phone and drag in Zig's stack-trace machinery; cross-compiling them to
    // aarch64-ios-simulator fails on `undefined symbol:
    // __dyld_get_image_header_containing_address` long after the library itself is
    // fine. A host embedding libzb wants this step.
    const lib_step = b.step("lib", "Build ONLY libzbcore (what a phone or a host embeds)");
    lib_step.dependOn(&b.addInstallArtifact(lib, .{}).step);

    // The orchestration demo (consumer #4): the loop against the live stack.
    const demo_mod = b.createModule(.{
        .root_source_file = b.path("src/demo.zig"),
        .target = target,
        .optimize = optimize,
    });
    demo_mod.addImport("c", c_mod);
    // §10dq: the wire grammar is compiled in — the same bytes the bridge embeds.
    demo_mod.addAnonymousImport("grammar", .{ .root_source_file = b.path("../src/grammar.json") });
    demo_mod.addImport("nats", nats_dep.module("nats"));
    demo_mod.addImport("msgpack", msgpack_dep.module("msgpack"));
    // §10iq: vendored, or the host's — never both.
    if (vendor) {
        addVendored(b, demo_mod, sqlite_dep, zstd_dep, &zstd_srcs, arch_include, android_api);
    } else {
        demo_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
        demo_mod.linkSystemLibrary("sqlite3", .{});
        demo_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
        demo_mod.linkSystemLibrary("zstd", .{});
    }
    demo_mod.link_libc = true;
    demo_mod.addImport("duckdb", duckdb_mod);
    const demo = b.addExecutable(.{ .name = "zb-demo", .root_module = demo_mod });
    b.installArtifact(demo);

    // The soak: the same client, driven for an hour. Its own binary because it is a
    // test harness with a ledger, not a demo.
    const soak_mod = b.createModule(.{
        .root_source_file = b.path("src/soak.zig"),
        .target = target,
        .optimize = optimize,
    });
    soak_mod.addImport("c", c_mod);
    // §10dq: the wire grammar is compiled in — the same bytes the bridge embeds.
    soak_mod.addAnonymousImport("grammar", .{ .root_source_file = b.path("../src/grammar.json") });
    soak_mod.addImport("nats", nats_dep.module("nats"));
    soak_mod.addImport("msgpack", msgpack_dep.module("msgpack"));
    // §10iq: vendored, or the host's — never both.
    if (vendor) {
        addVendored(b, soak_mod, sqlite_dep, zstd_dep, &zstd_srcs, arch_include, android_api);
    } else {
        soak_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
        soak_mod.linkSystemLibrary("sqlite3", .{});
        soak_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
        soak_mod.linkSystemLibrary("zstd", .{});
    }
    soak_mod.link_libc = true;
    soak_mod.addImport("duckdb", duckdb_mod);
    b.installArtifact(b.addExecutable(.{ .name = "zb-soak", .root_module = soak_mod }));

    // `zb sync` (§10fl): the replica as a command — the micro-VM worker's boot.
    const zb_mod = b.createModule(.{
        .root_source_file = b.path("src/zb.zig"),
        .target = target,
        .optimize = optimize,
    });
    zb_mod.addImport("c", c_mod);
    zb_mod.addAnonymousImport("grammar", .{ .root_source_file = b.path("../src/grammar.json") });
    zb_mod.addImport("nats", nats_dep.module("nats"));
    zb_mod.addImport("msgpack", msgpack_dep.module("msgpack"));
    // §10iq: vendored, or the host's — never both.
    if (vendor) {
        addVendored(b, zb_mod, sqlite_dep, zstd_dep, &zstd_srcs, arch_include, android_api);
    } else {
        zb_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
        zb_mod.linkSystemLibrary("sqlite3", .{});
        zb_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
        zb_mod.linkSystemLibrary("zstd", .{});
    }
    zb_mod.link_libc = true;
    zb_mod.addImport("duckdb", duckdb_mod);
    b.installArtifact(b.addExecutable(.{ .name = "zb", .root_module = zb_mod }));

    // `zb-respond config.json`: a responder with no Python — questions forwarded to a local
    // HTTP service (a routing engine), answers back to the askers.
    const respond_mod = b.createModule(.{
        .root_source_file = b.path("src/respond.zig"),
        .target = target,
        .optimize = optimize,
    });
    respond_mod.addImport("c", c_mod);
    respond_mod.addAnonymousImport("grammar", .{ .root_source_file = b.path("../src/grammar.json") });
    respond_mod.addImport("nats", nats_dep.module("nats"));
    respond_mod.addImport("msgpack", msgpack_dep.module("msgpack"));
    if (vendor) {
        addVendored(b, respond_mod, sqlite_dep, zstd_dep, &zstd_srcs, arch_include, android_api);
    } else {
        respond_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
        respond_mod.linkSystemLibrary("sqlite3", .{});
        respond_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
        respond_mod.linkSystemLibrary("zstd", .{});
    }
    respond_mod.link_libc = true;
    respond_mod.addImport("duckdb", duckdb_mod);
    const respond_exe = b.addExecutable(.{ .name = "zb-respond", .root_module = respond_mod });
    b.installArtifact(respond_exe);
    // `zig build respond` — the responder alone (deploy/build-linux.sh, for a server).
    const respond_step = b.step("respond", "Build ONLY zb-respond (the native responder)");
    respond_step.dependOn(&b.addInstallArtifact(respond_exe, .{}).step);

    const tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests (ZB_LIVE=1 adds the live transport test)");
    test_step.dependOn(&run_tests.step);
}

/// The embedded CA bundle (src/roots/cacert.pem) is refreshed by hand, before a release
/// (scripts/refresh-roots.sh): a phone build over six months past Mozilla's date says so.
fn warnStaleRoots(b: *std.Build) void {
    const io = b.graph.io;
    const text = std.Io.Dir.cwd().readFileAlloc(io, b.pathFromRoot("src/roots/cacert.date"), b.allocator, .limited(64)) catch {
        std.debug.print("warning: src/roots/cacert.date is missing: run scripts/refresh-roots.sh\n", .{});
        return;
    };
    const asof = std.fmt.parseInt(i64, std.mem.trim(u8, text, " \n\r\t"), 10) catch return;
    const age_days = @divTrunc(std.Io.Clock.real.now(io).toSeconds() - asof, 86400);
    if (age_days > 182) {
        std.debug.print("warning: the CA bundle iOS and Android carry is {d} days old: run scripts/refresh-roots.sh before a release\n", .{age_days});
    }
}

