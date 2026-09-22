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

    // §10ig: the PostgreSQL replica engine (§10fd) as a build-time switch, like
    // DuckDB's but ON by default — every desktop build has had it and nothing should
    // change for them. It exists for the phone: libpq-fe.h is not in the iOS SDK and
    // drags a hosted libc through translate-c, so a build that can never open a
    // PostgreSQL replica still had to find a PostgreSQL client library to compile.
    const with_libpq = b.option(bool, "libpq", "Build the PostgreSQL storage engine (needs libpq)") orelse true;

    const translate_c = b.addTranslateC(.{
        .root_source_file = b.path(if (with_libpq) "src/sqlite_includes.h" else "src/sqlite_includes_nopq.h"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "include" }) });
    // libzstd for DICTIONARY frames (§10x): std.compress.zstd parses a dictionary
    // id but cannot use one; plain frames still decode through std.
    const zstd_prefix: []const u8 = if (builtin.os.tag == .macos) "/opt/homebrew/opt/zstd" else "/usr";
    translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "include" }) });
    // libpq for the PostgreSQL replica engine (§10fd): a client's storage may be a
    // PostgreSQL server instead of a SQLite file — the micro-VM case.
    //
    const pq_prefix: []const u8 = b.option([]const u8, "libpq-prefix", "System libpq prefix") orelse
        (if (builtin.os.tag == .macos) "/opt/homebrew/opt/libpq" else "/usr");
    if (with_libpq) {
        translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ pq_prefix, "include" }) });
        translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ pq_prefix, "include", "postgresql" }) });
    }
    const c_mod = translate_c.createModule();

    // §10fl: the DuckDB engine, a build-time switch. Off by default and always off
    // on a phone: libduckdb is a 50 MB analytics library the micro-VM worker links
    // (Homebrew keg on macOS, the release zip unpacked under a prefix on Linux) and
    // nothing else needs. On, `duckdb.h` is translated as its own module.
    const with_duckdb = b.option(bool, "duckdb", "Build the DuckDB storage engine (needs libduckdb)") orelse false;
    const duckdb_prefix: []const u8 = b.option([]const u8, "duckdb-prefix", "System DuckDB prefix") orelse
        (if (builtin.os.tag == .macos) "/opt/homebrew/opt/duckdb" else "/usr/local");
    const build_opts = b.addOptions();
    build_opts.addOption(bool, "duckdb", with_duckdb);
    build_opts.addOption(bool, "libpq", with_libpq);
    const duckdb_mod: *std.Build.Module = if (with_duckdb) blk: {
        const tc = b.addTranslateC(.{
            .root_source_file = b.path("src/duckdb_includes.h"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        tc.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ duckdb_prefix, "include" }) });
        break :blk tc.createModule();
    } else b.createModule(.{ .root_source_file = b.path("src/duckdb_stub.zig"), .target = target, .optimize = optimize });

    const nats_dep = b.dependency("nats", .{ .target = target, .optimize = optimize });
    const msgpack_dep = b.dependency("zig_msgpack", .{ .target = target, .optimize = optimize });

    const mod = b.createModule(.{
        .root_source_file = b.path("src/capi.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("c", c_mod);
    // §10dq: the wire grammar is compiled in — the same bytes the bridge embeds.
    mod.addAnonymousImport("grammar", .{ .root_source_file = b.path("../src/grammar.json") });
    mod.addImport("nats", nats_dep.module("nats"));
    mod.addImport("msgpack", msgpack_dep.module("msgpack"));
    mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
    mod.linkSystemLibrary("sqlite3", .{});
    mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
    mod.linkSystemLibrary("zstd", .{});
    if (with_libpq) {
        mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ pq_prefix, "lib" }) });
        mod.linkSystemLibrary("pq", .{});
    }
    mod.link_libc = true;
    mod.addImport("duckdb", duckdb_mod);
    mod.addOptions("build_options", build_opts);
    if (with_duckdb) {
        mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ duckdb_prefix, "lib" }) });
        mod.linkSystemLibrary("duckdb", .{});
    }

    // The C-ABI shared library: one JSON dispatch entrypoint (zb_call) +
    // zb_free. Hosts: Python (ctypes), and later Dart/Swift/Kotlin/.NET FFI.
    const lib = b.addLibrary(.{
        .name = "zbcore",
        .root_module = mod,
        .linkage = .dynamic,
    });
    b.installArtifact(lib);

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
    demo_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
    demo_mod.linkSystemLibrary("sqlite3", .{});
    demo_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
    demo_mod.linkSystemLibrary("zstd", .{});
    if (with_libpq) {
        demo_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ pq_prefix, "lib" }) });
        demo_mod.linkSystemLibrary("pq", .{});
    }
    demo_mod.link_libc = true;
    demo_mod.addImport("duckdb", duckdb_mod);
    demo_mod.addOptions("build_options", build_opts);
    if (with_duckdb) {
        demo_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ duckdb_prefix, "lib" }) });
        demo_mod.linkSystemLibrary("duckdb", .{});
    }
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
    soak_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
    soak_mod.linkSystemLibrary("sqlite3", .{});
    soak_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
    soak_mod.linkSystemLibrary("zstd", .{});
    if (with_libpq) {
        soak_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ pq_prefix, "lib" }) });
        soak_mod.linkSystemLibrary("pq", .{});
    }
    soak_mod.link_libc = true;
    soak_mod.addImport("duckdb", duckdb_mod);
    soak_mod.addOptions("build_options", build_opts);
    if (with_duckdb) {
        soak_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ duckdb_prefix, "lib" }) });
        soak_mod.linkSystemLibrary("duckdb", .{});
    }
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
    zb_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
    zb_mod.linkSystemLibrary("sqlite3", .{});
    zb_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
    zb_mod.linkSystemLibrary("zstd", .{});
    if (with_libpq) {
        zb_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ pq_prefix, "lib" }) });
        zb_mod.linkSystemLibrary("pq", .{});
    }
    zb_mod.link_libc = true;
    zb_mod.addImport("duckdb", duckdb_mod);
    zb_mod.addOptions("build_options", build_opts);
    if (with_duckdb) {
        zb_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ duckdb_prefix, "lib" }) });
        zb_mod.linkSystemLibrary("duckdb", .{});
    }
    b.installArtifact(b.addExecutable(.{ .name = "zb", .root_module = zb_mod }));

    const tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests (ZB_LIVE=1 adds the live transport test)");
    test_step.dependOn(&run_tests.step);
}
