const std = @import("std");
const builtin = @import("builtin");

/// Link vendored or system libpq to an executable.
///
/// Options:
///   -Dvendored-libpq=true   Use pre-built libs/libpq-install/ (requires prior build of libpq)
///   -Dlibpq-prefix=/path    Override the system libpq prefix (include/ and lib/ under it)
///
/// Auto-detection for system libpq:
///   macOS  → /opt/homebrew/opt/libpq  (brew install libpq)
///   Linux  → /usr                     (apt install libpq-dev)
fn linkLibpq(exe: *std.Build.Step.Compile, b: *std.Build) void {
    const use_vendored = b.option(bool, "vendored-libpq", "Use vendored libpq from libs/libpq-install/") orelse false;

    if (use_vendored) {
        exe.root_module.addIncludePath(b.path("libs/libpq-install/include"));
        exe.root_module.addLibraryPath(b.path("libs/libpq-install/lib"));
        exe.root_module.addObjectFile(b.path("libs/libpq-install/lib/libpgcommon.a"));
        exe.root_module.addObjectFile(b.path("libs/libpq-install/lib/libpgport.a"));
        exe.root_module.addObjectFile(b.path("libs/libpq-install/lib/libpq.a"));
        std.debug.print("Using vendored libpq from libs/libpq-install\n", .{});
    } else {
        const default_prefix: []const u8 = if (builtin.os.tag == .macos)
            "/opt/homebrew/opt/libpq"
        else
            "/usr";
        const prefix = b.option([]const u8, "libpq-prefix", "System libpq prefix (default: /opt/homebrew/opt/libpq on macOS, /usr on Linux)") orelse default_prefix;

        const include_path = b.pathJoin(&.{ prefix, "include" });
        const lib_path = b.pathJoin(&.{ prefix, "lib" });
        exe.root_module.addSystemIncludePath(.{ .cwd_relative = include_path });
        exe.root_module.addLibraryPath(.{ .cwd_relative = lib_path });
        exe.root_module.linkSystemLibrary("pq", .{});
        std.debug.print("Using system libpq from {s}\n", .{prefix});
    }

    exe.root_module.link_libc = true;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("bridge", .{
        .root_source_file = b.path("src/bridge.zig"),
        .target = target,
    });

    const msgpack = b.dependency("zig_msgpack", .{
        .target = target,
        .optimize = optimize,
    });

    // Vendored g41797/mailbox (no external deps)
    const mailbox_mod = b.addModule("mailbox", .{
        .root_source_file = b.path("src/mailbox_vendor/mailbox.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Vendored g41797/nats patched for Zig 0.16:
    //   - Formatter.zig: std.Io.Writer.fixed → std.io.fixedBufferStream
    //   - Conn.zig: zul UUID → std.crypto.random
    const nats_mod = b.addModule("nats", .{
        .root_source_file = b.path("src/nats_vendor/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "mailbox", .module = mailbox_mod },
        },
    });

    // Create local zstd module (links system libzstd)
    const zstd_mod = b.addModule("zstd", .{
        .root_source_file = b.path("src/zstd.zig"),
        .target = target,
    });

    const exe = b.addExecutable(.{
        .name = "bridge",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bridge.zig"),

            .target = target,
            .optimize = optimize,

            .imports = &.{
                .{ .name = "bridge", .module = mod },
                .{ .name = "msgpack", .module = msgpack.module("msgpack") },
                .{ .name = "zstd", .module = zstd_mod },
                .{ .name = "nats", .module = nats_mod },
            },
        }),
    });

    // -Dstatic=true: produce a fully static binary using .a archives from the
    // system prefix.  Requires the builder to have installed the *-static or
    // *-dev packages that ship libssl.a, libcrypto.a, libzstd.a, libpq.a
    // (e.g. on Alpine: openssl-libs-static, zstd-static, postgresql-dev).
    const static_build = b.option(bool, "static", "Produce a fully static binary (links .a archives, no .so at runtime)") orelse false;
    if (static_build) exe.linkage = .static;

    // Link OpenSSL (required for pure Zig NATS TLS support)
    exe.root_module.linkSystemLibrary("ssl", .{});
    exe.root_module.linkSystemLibrary("crypto", .{});

    // Link system libzstd (for compression)
    exe.root_module.linkSystemLibrary("zstd", .{});

    // Link vendored libpq
    linkLibpq(exe, b);

    b.installArtifact(exe);

    // A top-level step to build and run the application with `zig build run`
    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // ===== Test executables =====
    // Creates an executable that will run `test` blocks from the module.
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    // A run step that will run the test executable.
    const run_mod_tests = b.addRunArtifact(mod_tests);

    // Creates an executable that will run `test` blocks from the executable's
    // root module. Note that test executables only test one module at a time,
    // hence why we have to create two separate ones.
    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    // A run step that will run the second test executable.
    const run_exe_tests = b.addRunArtifact(exe_tests);

    // A top level step for running all tests. dependOn can be called multiple
    // times and since the two run steps do not depend on one another, this will
    // make the two of them run in parallel.
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    // Clean nats build artifacts
    const clean_nats_step = b.step("clean-nats", "Clean nats.c build artifacts");
    const rm_nats_build = b.addSystemCommand(&.{ "rm", "-rf", "libs/nats.c/build", "libs/nats-install" });
    clean_nats_step.dependOn(&rm_nats_build.step);

    // ===== Test executables =====
    // Old lalinsky/nats.zig test files removed (obsolete trial code)
    // If needed, new tests for g41797/nats can be added here

    // ===== Commented out test executables - files moved to src/test_files/ =====
    // Uncomment and update paths if needed for testing
    //
    // // NATS test executable
    // const nats_test = b.addExecutable(.{
    //     .name = "nats_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/nats_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //     }),
    // });
    // nats_test.addIncludePath(b.path("libs/nats-install/include"));
    // nats_test.addLibraryPath(b.path("libs/nats-install/lib"));
    // nats_test.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // nats_test.linkLibC();
    // b.installArtifact(nats_test);
    //
    // const nats_test_step = b.step("nats-test", "Test NATS connection");
    // const nats_test_run = b.addRunArtifact(nats_test);
    // nats_test_run.step.dependOn(b.getInstallStep());
    // nats_test_step.dependOn(&nats_test_run.step);
    //
    // // PostgreSQL test executable
    // const pg_test = b.addExecutable(.{
    //     .name = "pg_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/pg_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //         },
    //     }),
    // });
    // b.installArtifact(pg_test);
    //
    // const pg_test_step = b.step("pg-test", "Test PostgreSQL connection");
    // const pg_test_run = b.addRunArtifact(pg_test);
    // pg_test_run.step.dependOn(b.getInstallStep());
    // pg_test_step.dependOn(&pg_test_run.step);
    //
    // // CDC Demo executable
    // const cdc_demo = b.addExecutable(.{
    //     .name = "cdc_demo",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/cdc_demo.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //             .{ .name = "msgpack", .module = msgpack.module("msgpack") },
    //         },
    //     }),
    // });
    // cdc_demo.addIncludePath(b.path("libs/nats-install/include"));
    // cdc_demo.addLibraryPath(b.path("libs/nats-install/lib"));
    // cdc_demo.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // cdc_demo.linkLibC();
    // b.installArtifact(cdc_demo);
    //
    // const cdc_demo_step = b.step("cdc-demo", "Run CDC bridge demo");
    // const cdc_demo_run = b.addRunArtifact(cdc_demo);
    // cdc_demo_run.step.dependOn(b.getInstallStep());
    // cdc_demo_step.dependOn(&cdc_demo_run.step);
    //
    // // CDC Load Test executable
    // const cdc_load_test = b.addExecutable(.{
    //     .name = "cdc_load_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/cdc_load_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //             .{ .name = "msgpack", .module = msgpack.module("msgpack") },
    //         },
    //     }),
    // });
    // // Link NATS
    // cdc_load_test.addIncludePath(b.path("libs/nats-install/include"));
    // cdc_load_test.addLibraryPath(b.path("libs/nats-install/lib"));
    // cdc_load_test.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // // Link vendored libpq
    // linkLibpq(cdc_load_test, b);
    // b.installArtifact(cdc_load_test);
    //
    // const cdc_load_test_step = b.step("load-test", "Run CDC load test");
    // const cdc_load_test_run = b.addRunArtifact(cdc_load_test);
    // cdc_load_test_run.step.dependOn(b.getInstallStep());
    // cdc_load_test_step.dependOn(&cdc_load_test_run.step);
    //
    // // WAL Stream Test executable
    // const wal_stream_test = b.addExecutable(.{
    //     .name = "wal_stream_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/wal_stream_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //         },
    //     }),
    // });
    // // Link vendored libpq for replication protocol
    // linkLibpq(wal_stream_test, b);
    // b.installArtifact(wal_stream_test);
    //
    // const wal_stream_test_step = b.step("wal-stream-test", "Test WAL streaming with libpq");
    // const wal_stream_test_run = b.addRunArtifact(wal_stream_test);
    // wal_stream_test_run.step.dependOn(b.getInstallStep());
    // wal_stream_test_step.dependOn(&wal_stream_test_run.step);

    // // Connection State Test
    // const conn_state_test = b.addExecutable(.{
    //     .name = "connection_state_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/connection_state_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //         },
    //     }),
    // });
    // b.installArtifact(conn_state_test);
    //
    // const conn_state_test_step = b.step("conn-test", "Test connection state handling");
    // const conn_state_test_run = b.addRunArtifact(conn_state_test);
    // conn_state_test_run.step.dependOn(b.getInstallStep());
    // conn_state_test_step.dependOn(&conn_state_test_run.step);
}
