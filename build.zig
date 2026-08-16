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
    
    // Every name the bridge puts on the wire comes from here. `.?` on purpose: a key
    // missing from topology.json must fail the build, not silently fall back to a
    // literal — a literal is exactly how the bridge and nats-init drifted apart.
    const streams = parsed_topology.object.get("streams").?.object;
    topology_opts.addOption([]const u8, "stream_cdc", streams.get("cdc").?.string);
    topology_opts.addOption([]const u8, "stream_init", streams.get("init").?.string);
    topology_opts.addOption([]const u8, "stream_mutations", streams.get("mutations").?.string);
    topology_opts.addOption([]const u8, "stream_requests", streams.get("requests").?.string);

    const subjects = parsed_topology.object.get("subjects").?.object;
    topology_opts.addOption([]const u8, "subject_cdc_prefix", subjects.get("cdc_prefix").?.string);
    topology_opts.addOption([]const u8, "subject_init_prefix", subjects.get("init_prefix").?.string);
    topology_opts.addOption([]const u8, "subject_mutations_prefix", subjects.get("mutations_prefix").?.string);
    topology_opts.addOption([]const u8, "mutation_pattern", subjects.get("mutation_pattern").?.string);
    topology_opts.addOption([]const u8, "mutation_error_prefix", subjects.get("mutation_error_prefix").?.string);
    topology_opts.addOption([]const u8, "mutation_error_pattern", subjects.get("mutation_error_pattern").?.string);
    topology_opts.addOption([]const u8, "snapshot_request", subjects.get("snapshot_request").?.string);
    topology_opts.addOption([]const u8, "schema_request", subjects.get("schema_request").?.string);
    topology_opts.addOption([]const u8, "snapshot_data_pattern", subjects.get("snapshot_data_pattern").?.string);
    topology_opts.addOption([]const u8, "snapshot_start_pattern", subjects.get("snapshot_start_pattern").?.string);
    topology_opts.addOption([]const u8, "snapshot_error_pattern", subjects.get("snapshot_error_pattern").?.string);
    topology_opts.addOption([]const u8, "snapshot_meta_pattern", subjects.get("snapshot_meta_pattern").?.string);
    topology_opts.addOption([]const u8, "snapshot_schema_pattern", subjects.get("snapshot_schema_pattern").?.string);
    
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

    exe.root_module.link_libc = true;
    linkLibpq(exe, b, prefix);

    b.installArtifact(exe);

    const gc_exe = b.addExecutable(.{
        .name = "gc",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/gc.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "c", .module = c_mod },
            },
        }),
    });
    gc_exe.root_module.link_libc = true;
    linkLibpq(gc_exe, b, prefix);
    b.installArtifact(gc_exe);

    const gc_run_cmd = b.addRunArtifact(gc_exe);
    gc_run_cmd.step.dependOn(b.getInstallStep());
    const run_gc_step = b.step("run-gc", "Run the Garbage Collector Sidecar");
    run_gc_step.dependOn(&gc_run_cmd.step);

    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    run_step.dependOn(&run_cmd.step);

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // ===== Test executables =====
    // Creates an executable that will run `test` blocks from the module.
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    linkLibpq(mod_tests, b, prefix);

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

}
