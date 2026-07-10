//! Per-call state. RawCall drives one h2 stream through the gRPC call
//! lifecycle; Call(M) (Task 9) is the typed wrapper on top.

const std = @import("std");
const h2 = @import("zig_http2");
const status_mod = @import("status.zig");
const metadata_mod = @import("metadata.zig");
const frame = @import("frame.zig");
const channel_mod = @import("channel.zig");

pub const Status = status_mod.Status;
pub const Metadata = metadata_mod.Metadata;

pub const CallOptions = struct {
    metadata: []const Metadata.Entry = &.{},
    /// Encoded as `grpc-timeout`; enforced by the SERVER. v1 has no local
    /// deadline (a blocked recv cannot be interrupted — see the spec).
    timeout_ns: ?u64 = null,
    /// For `Channel.unary` only: receives the final Status on non-OK.
    status_out: ?*Status = null,
};

pub const RawCall = struct {
    chan: *channel_mod.Channel,
    stream: *h2.Stream,
    assembler: frame.Assembler,
    /// Owns response headers/trailers/status message until `deinit`.
    arena_state: std.heap.ArenaAllocator,
    state: State = .awaiting_headers,
    resp_headers: ?Metadata = null,
    resp_trailers: Metadata = .{},
    stat: ?Status = null,
    send_closed: bool = false,

    const State = enum { awaiting_headers, open, done };

    pub fn init(chan: *channel_mod.Channel, stream: *h2.Stream) RawCall {
        return .{
            .chan = chan,
            .stream = stream,
            .assembler = frame.Assembler.init(chan.gpa, chan.opts.max_recv_message_size),
            .arena_state = std.heap.ArenaAllocator.init(chan.gpa),
        };
    }

    /// Frames `msg` (5-byte prefix) and sends it as flow-controlled DATA.
    /// Blocks while the send window is empty. One sender thread per call.
    pub fn sendMessage(self: *RawCall, msg: []const u8) !void {
        if (msg.len > self.chan.opts.max_send_message_size) return error.MessageTooLarge;
        const buf = try self.chan.gpa.alloc(u8, frame.prefix_len + msg.len);
        defer self.chan.gpa.free(buf);
        buf[0..frame.prefix_len].* = frame.encodePrefix(@intCast(msg.len));
        @memcpy(buf[frame.prefix_len..], msg);
        try self.stream.send(buf, false);
    }

    /// Half-closes the request direction (empty DATA + END_STREAM).
    pub fn closeSend(self: *RawCall) !void {
        if (self.send_closed) return;
        self.send_closed = true;
        try self.stream.send("", true);
    }

    /// Tells the server to cancel (RST_STREAM CANCEL) and releases the
    /// stream's concurrency slot. Does NOT interrupt a concurrently blocked
    /// recv on another thread — see the spec's threading section.
    pub fn cancel(self: *RawCall) void {
        self.stream.cancel() catch {};
        if (self.stat == null) self.stat = .{ .code = .cancelled, .message = "cancelled by client" };
        self.state = .done;
    }

    /// Releases the call. Must not race a concurrent sendMessage/recvMessage.
    pub fn deinit(self: *RawCall) void {
        self.stream.close();
        self.assembler.deinit();
        self.arena_state.deinit();
    }
};
