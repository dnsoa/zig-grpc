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

const server_mod = @import("server.zig");
pub const server = server_mod;

const call_mod = @import("call.zig");
const channel_mod = @import("channel.zig");
pub const CallOptions = call_mod.CallOptions;
pub const RawCall = call_mod.RawCall;
pub const Call = call_mod.Call;
pub const Channel = channel_mod.Channel;

test {
    _ = status_mod;
    _ = metadata_mod;
    _ = frame;
    _ = codec_mod;
    _ = server_mod;
    _ = call_mod;
    _ = channel_mod;
}

test "scaffold compiles" {
    const h2 = @import("zig_http2");
    _ = h2;
    try std.testing.expect(true);
}
