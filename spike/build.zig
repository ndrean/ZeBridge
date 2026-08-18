const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const nats = b.dependency("nats", .{ .target = target, .optimize = optimize });

    const exe = b.addExecutable(.{
        .name = "spike",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "nats", .module = nats.module("nats") }},
        }),
    });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    b.step("run", "run the spike").dependOn(&run.step);
}
