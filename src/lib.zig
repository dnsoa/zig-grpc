//! zig-grpc — gRPC client/server on top of zig-http2.
//! Core is bytes-in/bytes-out; message codecs plug in via comptime contracts.

const std = @import("std");

const status_mod = @import("status.zig");
pub const Code = status_mod.Code;
pub const Status = status_mod.Status;

const metadata_mod = @import("metadata.zig");
pub const Metadata = metadata_mod.Metadata;

pub const frame = @import("frame.zig");

const codec_mod = @import("codec.zig");
pub const Method = codec_mod.Method;

test {
    _ = status_mod;
    _ = metadata_mod;
    _ = frame;
    _ = codec_mod;
}

test "scaffold compiles" {
    const h2 = @import("zig_http2");
    _ = h2;
    try std.testing.expect(true);
}
