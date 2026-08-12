const std = @import("std");
const builtin = @import("builtin");

/// Link system libpq to a compile step.
///
/// Headers are handled separately by the translate-c step; this only needs the
/// library itself. The runtime is always a container we build (see Dockerfile),
/// so the distro's libpq is the dependency pin and picks up its own CVE fixes.
///
///   -Dlibpq-prefix=/path    Override the libpq prefix (include/ and lib/ under it)
///
/// Auto-detection:
///   macOS  → /opt/homebrew/opt/libpq  (brew install libpq)
///   Linux  → /usr                     (apk add postgresql-dev / apt install libpq-dev)
fn linkLibpq(compile: *std.Build.Step.Compile, b: *std.Build, prefix: []const u8) void {
    const lib_path = b.pathJoin(&.{ prefix, "lib" });
    compile.root_module.addLibraryPath(.{ .cwd_relative = lib_path });
    compile.root_module.linkSystemLibrary("pq", .{});
    compile.root_module.link_libc = true;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const default_prefix: []const u8 = if (builtin.os.tag == .macos)
        "/opt/homebrew/opt/libpq"
    else
        "/usr";
    const prefix = b.option([]const u8, "libpq-prefix", "System libpq prefix (default: /opt/homebrew/opt/libpq on macOS, /usr on Linux)") orelse default_prefix;

    std.debug.print("Using system libpq from {s}\n", .{prefix});

    // Translate the C headers once into a real module. Replaces @cImport, which
    // gave every import site its own incompatible copy of the C types and does not
    // work with incremental compilation.
    const translate_c = b.addTranslateC(.{
        .root_source_file = b.path("src/c_includes.h"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    // Header layout differs by packaging, so offer both and let the compiler pick:
    //   Homebrew keg   → {prefix}/include/libpq-fe.h
    //   Alpine/Debian  → {prefix}/include/postgresql/libpq-fe.h   (pg_config --includedir)
    // A path that does not exist is simply ignored.
    translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "include" }) });
    translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "include", "postgresql" }) });
    const c_mod = translate_c.createModule();

    const mod = b.addModule("bridge", .{
        .root_source_file = b.path("src/bridge.zig"),
        .target = target,
    });

    const msgpack = b.dependency("zig_msgpack", .{
        .target = target,
        .optimize = optimize,
    });

    // Parse topology.json and generate build options
    const topology_file = @embedFile("topology.json");
    var parsed_topology = std.json.parseFromSliceLeaky(std.json.Value, b.allocator, topology_file, .{}) catch |err| {
        std.debug.panic("Failed to parse topology.json: {any}\n", .{err});
    };
    const topology_opts = b.addOptions();
    
    const streams = parsed_topology.object.get("streams").?.object;
    topology_opts.addOption([]const u8, "stream_cdc", streams.get("cdc").?.string);
    topology_opts.addOption([]const u8, "stream_init", streams.get("init").?.string);
    topology_opts.addOption([]const u8, "stream_schema", streams.get("schema").?.string);
    
    const subjects = parsed_topology.object.get("subjects").?.object;
    topology_opts.addOption([]const u8, "subject_cdc_prefix", subjects.get("cdc_prefix").?.string);
    topology_opts.addOption([]const u8, "subject_init_prefix", subjects.get("init_prefix").?.string);
    topology_opts.addOption([]const u8, "subject_schema_prefix", subjects.get("schema_prefix").?.string);
    topology_opts.addOption([]const u8, "snapshot_request", subjects.get("snapshot_request").?.string);
    
    const kv = parsed_topology.object.get("kv").?.object;
    topology_opts.addOption([]const u8, "kv_schemas", kv.get("schemas").?.string);
    topology_opts.addOption([]const u8, "kv_snapshots", kv.get("snapshots").?.string);

    const topology_mod = topology_opts.createModule();

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

    // The "bridge" module shares its root with the exe, so it needs the same imports —
    // without these, `zig build test` cannot compile any module that uses msgpack/nats.
    mod.addImport("msgpack", msgpack.module("msgpack"));
    mod.addImport("nats", nats_mod);
    mod.addImport("c", c_mod);
    mod.addImport("topology", topology_mod);

    const exe = b.addExecutable(.{
        .name = "bridge",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bridge.zig"),

            .target = target,
            .optimize = optimize,

            .imports = &.{
                .{ .name = "bridge", .module = mod },
                .{ .name = "msgpack", .module = msgpack.module("msgpack") },
                .{ .name = "nats", .module = nats_mod },
                .{ .name = "c", .module = c_mod },
                .{ .name = "topology", .module = topology_mod },
            },
        }),
    });

    // Link OpenSSL (required for pure Zig NATS TLS support)
    exe.root_module.linkSystemLibrary("ssl", .{});
    exe.root_module.linkSystemLibrary("crypto", .{});

    linkLibpq(exe, b, prefix);

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
    linkLibpq(mod_tests, b, prefix);
    mod_tests.root_module.linkSystemLibrary("ssl", .{});
    mod_tests.root_module.linkSystemLibrary("crypto", .{});

    // A run step that will run the test executable.
    const run_mod_tests = b.addRunArtifact(mod_tests);

    // Creates an executable that will run `test` blocks from the executable's
    // root module. Note that test executables only test one module at a time,
    // hence why we have to create two separate ones.
    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });
    linkLibpq(exe_tests, b, prefix);

    // A run step that will run the second test executable.
    const run_exe_tests = b.addRunArtifact(exe_tests);

    // A top level step for running all tests. dependOn can be called multiple
    // times and since the two run steps do not depend on one another, this will
    // make the two of them run in parallel.
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    // ===== Vendored NATS library's own test suite =====
    // Ported from upstream g41797/nats 0.0.3. The upstream repo targets Zig 0.15 and
    // does not build on 0.16, so src/nats_vendor/ is the only 0.16 copy — meaning its
    // tests have to run here rather than upstream. Kept off `zig build test` because
    // these exercise vendored third-party code, not the bridge.
    const nats_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/nats_vendor/root_tests.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "mailbox", .module = mailbox_mod },
            },
        }),
    });
    nats_tests.root_module.link_libc = true;

    const nats_test_step = b.step("test-nats", "Run the vendored NATS library's unit tests");
    nats_test_step.dependOn(&b.addRunArtifact(nats_tests).step);

    // Integration tests need a live NATS server on 4222 (see docker-compose.full.yml).
    const nats_int_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/nats_vendor/integration_tests.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "mailbox", .module = mailbox_mod },
            },
        }),
    });
    nats_int_tests.root_module.link_libc = true;

    const nats_int_step = b.step("test-nats-integration", "Run vendored NATS integration tests (requires a live server)");
    nats_int_step.dependOn(&b.addRunArtifact(nats_int_tests).step);

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
