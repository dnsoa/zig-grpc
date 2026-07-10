const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const h2_dep = b.dependency("zig_http2", .{ .target = target, .optimize = optimize });
    const h2_mod = h2_dep.module("zig_http2");

    // The public library module. Consumers do
    //   b.dependency("zig_grpc", .{}).module("zig_grpc")
    // and then `@import("zig_grpc")`.
    const mod = b.addModule("zig_grpc", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("zig_http2", h2_mod);

    // ---- `zig build test` ----
    const test_step = b.step("test", "Run unit tests");
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_mod.addImport("zig_http2", h2_mod);
    const run_tests = b.addRunArtifact(b.addTest(.{ .root_module = test_mod }));
    test_step.dependOn(&run_tests.step);

    // ---- `zig build interop` ----
    const interop_mod = b.createModule(.{
        .root_source_file = b.path("src/interop.zig"),
        .target = target,
        .optimize = optimize,
    });
    interop_mod.addImport("zig_grpc", mod);
    const interop_exe = b.addExecutable(.{ .name = "zig-grpc-interop", .root_module = interop_mod });
    const interop_step = b.step("interop", "Build the Go-interop client (see scripts/interop.sh)");
    interop_step.dependOn(&b.addInstallArtifact(interop_exe, .{}).step);
}
