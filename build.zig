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
/// Link system libzstd (compression side only: the generation producer
/// compresses chain objects; clients DEcompress via std.compress.zstd or
/// their runtime's native zstd — no C needed there).
fn linkZstd(compile: *std.Build.Step.Compile, b: *std.Build, prefix: []const u8) void {
    compile.root_module.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "lib" }) });
    compile.root_module.linkSystemLibrary("zstd", .{});
}

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
    std.debug.print("Using system zstd", .{});

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
    const zstd_prefix: []const u8 = if (builtin.os.tag == .macos) "/opt/homebrew/opt/zstd" else "/usr";
    translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ zstd_prefix, "include" }) });
    const c_mod = translate_c.createModule();

    // The sweeper's own, PostgreSQL only (src/c_includes_pg.h): it is a separate
    // binary, so it needs neither zstd's headers at build time nor libzstd at run
    // time. Exposed to it under the SAME module name "c", so src/c_imports.zig and
    // every call site are shared verbatim between the two executables.
    const pg_translate_c = b.addTranslateC(.{
        .root_source_file = b.path("src/c_includes_pg.h"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    pg_translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "include" }) });
    pg_translate_c.addSystemIncludePath(.{ .cwd_relative = b.pathJoin(&.{ prefix, "include", "postgresql" }) });
    const pg_c_mod = pg_translate_c.createModule();

    // The release version, from build.zig.zon: `bridge --version` and the packages.
    const build_options = b.addOptions();
    build_options.addOption([]const u8, "version", @import("build.zig.zon").version);
    // Off in every mode: only scripts/scenarios/receipt_faults.py's own binary turns it on
    // (-Dtest-faults=true). Off, the fault path is not compiled in at all.
    build_options.addOption(bool, "test_faults", b.option(bool, "test-faults", "Compile in ZB_TEST_FAULT fault injection (receipt_faults.py only)") orelse false);
    // Require a completed receipt for every write before the transaction commits.
    build_options.addOption(bool, "receipt_guard", b.option(bool, "receipt-guard", "Receipt completion guard (default true)") orelse true);

    const mod = b.addModule("bridge", .{
        .root_source_file = b.path("src/bridge.zig"),
        .target = target,
    });

    const msgpack = b.dependency("zig_msgpack", .{
        .target = target,
        .optimize = optimize,
    });

    // topology.json is no longer parsed here. It used to be @embedFile'd and turned into
    // build options, which baked every stream and subject name into the binary — so a
    // rename reached `nats-init` on `docker compose up` but not the bridge, which needed
    // a rebuild while logging the names as though it had just read them. It is read at
    // startup now; see src/topology.zig.

    // src/mailbox_vendor/ removed with src/nats_vendor/: it existed only to back that
    // client's message pool, and nothing in the bridge imported it directly.

    // lalinsky/nats.zig — std.Io transport, no C sockets. Replaces the vendored
    // g41797 client; see nats.zig/NOTES.md for the one local fix carried against it.
    const nats_dep = b.dependency("nats", .{ .target = target, .optimize = optimize });
    const nats_mod = nats_dep.module("nats");

    // The "bridge" module shares its root with the exe, so it needs the same imports —
    // without these, `zig build test` cannot compile any module that uses msgpack/nats.
    mod.addImport("msgpack", msgpack.module("msgpack"));
    mod.addImport("nats", nats_mod);
    mod.addImport("c", c_mod);
    mod.addOptions("build_options", build_options);
    // §10ke: `bridge --init-sql` prints the init SQL, so the templates travel inside
    // the binary. They live at the repository root, outside src/, where @embedFile
    // cannot reach: named imports can.
    mod.addAnonymousImport("init_core_sql", .{ .root_source_file = b.path("init.core.template.sql") });
    mod.addAnonymousImport("init_write_sql", .{ .root_source_file = b.path("init.write.template.sql") });

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
            },
        }),
    });
    exe.root_module.addOptions("build_options", build_options);

    exe.root_module.addAnonymousImport("init_core_sql", .{ .root_source_file = b.path("init.core.template.sql") });
    exe.root_module.addAnonymousImport("init_write_sql", .{ .root_source_file = b.path("init.write.template.sql") });
    exe.root_module.link_libc = true;
    linkLibpq(exe, b, prefix);
    linkZstd(exe, b, zstd_prefix);

    b.installArtifact(exe);

    const gc_exe = b.addExecutable(.{
        .name = "bridge_sweeper",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bridge_sweeper.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "c", .module = pg_c_mod },
            },
        }),
    });
    gc_exe.root_module.addOptions("build_options", build_options);
    gc_exe.root_module.link_libc = true;
    linkLibpq(gc_exe, b, prefix);
    // ⚠️ No linkZstd here: the sweeper references no zstd symbol (it only runs
    // DELETEs against PostgreSQL), and linking it put libzstd in the runtime
    // dependencies of every image built to run the sidecar alone.
    b.installArtifact(gc_exe);

    // The readings archiver's reader (examples/16-archive): pulls a range of a readings stream
    // into a CSV file, an order of magnitude faster than nats-py. Needs only the NATS client —
    // no libpq, no zstd — so the archiver's image carries one small binary.
    const fetch_exe = b.addExecutable(.{
        .name = "bridge_readings_fetch",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bridge_readings_fetch.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats_mod },
            },
        }),
    });
    b.installArtifact(fetch_exe);
    const fetch_step = b.step("readings-fetch", "Build only bridge_readings_fetch (the archiver's stream reader)");
    fetch_step.dependOn(&b.addInstallArtifact(fetch_exe, .{}).step);

    // The native readings archiver (archiver.zig): the reader above in-process, DuckDB for
    // the Parquet (opened at run time, from libzb's vendored header — no link, no extension),
    // SigV4 S3 over std.http. Two artifacts over one module: the command `bridge_archiver`
    // and the C ABI `libzbarchive` (include/zbarchive.h) for Go, Rust, Python, Swift hosts.
    // Neither needs libpq or zstd; neither is part of libzbcore (a mobile client never carries it).
    const duck_tc = b.addTranslateC(.{
        .root_source_file = b.path("src/duckdb_includes.h"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    duck_tc.addIncludePath(b.path("libzb/include-engines"));
    const duck_mod = duck_tc.createModule();
    const archiver_imports: []const std.Build.Module.Import = &.{
        .{ .name = "nats", .module = nats_mod },
        .{ .name = "duckdb", .module = duck_mod },
    };
    const archiver_exe = b.addExecutable(.{
        .name = "bridge_archiver",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bridge_archiver.zig"),
            .target = target,
            .optimize = optimize,
            .imports = archiver_imports,
            .link_libc = true,
        }),
    });
    b.installArtifact(archiver_exe);
    const archiver_lib = b.addLibrary(.{
        .name = "zbarchive",
        .linkage = .dynamic,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/archiver_capi.zig"),
            .target = target,
            .optimize = optimize,
            .imports = archiver_imports,
            .link_libc = true,
        }),
    });
    b.installArtifact(archiver_lib);
    b.getInstallStep().dependOn(&b.addInstallFile(b.path("include/zbarchive.h"), "include/zbarchive.h").step);
    const archiver_step = b.step("archiver", "Build only bridge_archiver and libzbarchive (the native readings archiver)");
    archiver_step.dependOn(&b.addInstallArtifact(archiver_exe, .{}).step);
    archiver_step.dependOn(&b.addInstallArtifact(archiver_lib, .{}).step);
    archiver_step.dependOn(&b.addInstallFile(b.path("include/zbarchive.h"), "include/zbarchive.h").step);

    // `zig build archive-core-wasm` — the archiver's decisions and the payload decoder for a
    // WebAssembly host (src/archive_core_wasm.zig): wasm32, no I/O. Not part of the default build.
    const archive_wasm = b.addExecutable(.{
        .name = "zb_archive_core",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/archive_core_wasm.zig"),
            .target = b.resolveTargetQuery(.{ .cpu_arch = .wasm32, .os_tag = .freestanding }),
            .optimize = .ReleaseSmall,
        }),
    });
    archive_wasm.entry = .disabled;
    archive_wasm.rdynamic = true;
    const archive_wasm_step = b.step("archive-core-wasm", "Build ONLY the archiver's core for WebAssembly (zig-out/wasm/zb_archive_core.wasm)");
    archive_wasm_step.dependOn(&b.addInstallArtifact(archive_wasm, .{ .dest_dir = .{ .override = .{ .custom = "wasm" } } }).step);

    // `zig build test-archiver` — the archiver's unit tests (no NATS, S3 or DuckDB needed).
    const archiver_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/archiver_capi.zig"),
            .target = target,
            .optimize = optimize,
            .imports = archiver_imports,
            .link_libc = true,
        }),
    });
    const archiver_core_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/archiver.zig"),
            .target = target,
            .optimize = optimize,
            .imports = archiver_imports,
            .link_libc = true,
        }),
    });
    const archiver_test_step = b.step("test-archiver", "Run the readings archiver's unit tests");
    archiver_test_step.dependOn(&b.addRunArtifact(archiver_tests).step);
    archiver_test_step.dependOn(&b.addRunArtifact(archiver_core_tests).step);

    // `zig build sweeper` — the sidecar alone. The default step installs both binaries
    // and `run-gc` depends on the whole install, so a loop that only touches
    // src/bridge_sweeper.zig was paying for the bridge's compile every time.
    const sweeper_step = b.step("sweeper", "Build only bridge_sweeper (the GC sidecar)");
    sweeper_step.dependOn(&b.addInstallArtifact(gc_exe, .{}).step);

    // The same for the daemon, so the pair reads symmetrically: `zig build bridge`.
    const bridge_step = b.step("bridge", "Build only the bridge daemon");
    bridge_step.dependOn(&b.addInstallArtifact(exe, .{}).step);

    const gc_run_cmd = b.addRunArtifact(gc_exe);
    gc_run_cmd.step.dependOn(b.getInstallStep());
    const run_gc_step = b.step("run-gc", "Run the Garbage Collector Sidecar");
    run_gc_step.dependOn(&gc_run_cmd.step);

    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    run_step.dependOn(&run_cmd.step);

    run_cmd.addPassthruArgs();

    // ===== Test executables =====
    // Creates an executable that will run `test` blocks from the module.
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    linkLibpq(mod_tests, b, prefix);
    // The producer's tests compress (§10gi): the module's test binary links libzstd
    // like the bridge does.
    linkZstd(mod_tests, b, zstd_prefix);

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
    test_step.dependOn(archiver_test_step);

    // The vendored NATS test steps (`test-nats`, `test-nats-integration`) are gone with
    // src/nats_vendor/. The library is a dependency now and carries its own suite:
    //
    //     cd nats.zig && zig build test        # 104 + 167 tests
    //     cd spike    && ./zig-out/bin/spike   # the five capabilities the bridge needs

}
