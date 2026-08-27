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

    const translate_c = b.addTranslateC(.{
        .root_source_file = b.path("src/sqlite_includes.h"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "include" }) });
    // libzstd for DICTIONARY frames (§10x): std.compress.zstd parses a dictionary
    // id but cannot use one; plain frames still decode through std.
    const zstd_prefix: []const u8 = if (builtin.os.tag == .macos) "/opt/homebrew/opt/zstd" else "/usr";
    translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "include" }) });
    const c_mod = translate_c.createModule();

    const nats_dep = b.dependency("nats", .{ .target = target, .optimize = optimize });
    const msgpack_dep = b.dependency("zig_msgpack", .{ .target = target, .optimize = optimize });

    const mod = b.createModule(.{
        .root_source_file = b.path("src/capi.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("c", c_mod);
    mod.addImport("nats", nats_dep.module("nats"));
    mod.addImport("msgpack", msgpack_dep.module("msgpack"));
    mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
    mod.linkSystemLibrary("sqlite3", .{});
    mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
    mod.linkSystemLibrary("zstd", .{});
    mod.link_libc = true;

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
    demo_mod.addImport("nats", nats_dep.module("nats"));
    demo_mod.addImport("msgpack", msgpack_dep.module("msgpack"));
    demo_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ sqlite_prefix, "lib" }) });
    demo_mod.linkSystemLibrary("sqlite3", .{});
    demo_mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "lib" }) });
    demo_mod.linkSystemLibrary("zstd", .{});
    demo_mod.link_libc = true;
    const demo = b.addExecutable(.{ .name = "zb-demo", .root_module = demo_mod });
    b.installArtifact(demo);

    const tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests (ZB_LIVE=1 adds the live transport test)");
    test_step.dependOn(&run_tests.step);
}
