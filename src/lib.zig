//! zig-grpc — gRPC client/server on top of zig-http2.
//! Core is bytes-in/bytes-out; message codecs plug in via comptime contracts.

const std = @import("std");

test "scaffold compiles" {
    const h2 = @import("zig_http2");
    _ = h2;
    try std.testing.expect(true);
}
