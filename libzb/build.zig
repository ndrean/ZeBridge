const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.createModule(.{
        .root_source_file = b.path("src/capi.zig"),
        .target = target,
        .optimize = optimize,
    });

    // The C-ABI shared library: one JSON dispatch entrypoint (zb_call) +
    // zb_free. Hosts: Python (ctypes), and later Dart/Swift/Kotlin/.NET FFI.
    const lib = b.addLibrary(.{
        .name = "zbcore",
        .root_module = mod,
        .linkage = .dynamic,
    });
    b.installArtifact(lib);

    const tests = b.addTest(.{ .root_module = mod });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}
