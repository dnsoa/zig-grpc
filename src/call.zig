//! Per-call state. RawCall drives one h2 stream through the gRPC call
//! lifecycle; Call(M) (Task 9) is the typed wrapper on top.

const std = @import("std");
const h2 = @import("zig_http2");
const status_mod = @import("status.zig");
const metadata_mod = @import("metadata.zig");
const frame = @import("frame.zig");
const channel_mod = @import("channel.zig");

const testing = std.testing;
const testutil = @import("testutil.zig");

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

    /// Reads the next response message into `arena`. Returns null once the
    /// server has finished (trailers received) — then `finish()` has the
    /// status. Transport faults are Zig errors; `stat` still carries the
    /// mapped gRPC status afterwards. One receiver thread per call.
    pub fn recvMessage(self: *RawCall, arena: std.mem.Allocator) !?[]u8 {
        while (true) {
            const maybe = self.assembler.next(arena) catch |e| {
                if (self.stat == null) self.stat = switch (e) {
                    error.MessageTooLarge => .{ .code = .resource_exhausted, .message = "message exceeds max_recv_message_size" },
                    error.CompressedUnsupported => .{ .code = .internal, .message = "compressed message but compression is unsupported" },
                    error.MalformedFrame => .{ .code = .internal, .message = "malformed message frame" },
                    else => .{ .code = .internal, .message = "receive failure" },
                };
                self.abandon();
                return e;
            };
            if (maybe) |m| return m;
            if (self.state == .done) {
                if (self.assembler.hasPartial() and (self.stat == null or self.stat.?.isOk())) {
                    self.stat = .{ .code = .internal, .message = "stream ended mid-message" };
                }
                return null;
            }
            try self.step();
        }
    }

    /// Blocks until the response HEADERS arrive; empty for Trailers-Only.
    /// The returned Metadata is owned by the call (valid until deinit).
    pub fn header(self: *RawCall) !Metadata {
        while (self.resp_headers == null and self.state == .awaiting_headers) try self.step();
        return self.resp_headers orelse .{};
    }

    /// Drains any remaining messages, then returns the final status. Intended
    /// after recvMessage returned null (for streams) or directly (unary
    /// convenience). NOTE: blocks until the server ends the stream.
    pub fn finish(self: *RawCall) !Status {
        var scratch = std.heap.ArenaAllocator.init(self.chan.gpa);
        defer scratch.deinit();
        while (self.state != .done) {
            _ = self.recvMessage(scratch.allocator()) catch break;
            _ = scratch.reset(.retain_capacity);
        }
        return self.stat orelse .{ .code = .internal, .message = "call ended without status" };
    }

    /// Trailer metadata (excluding grpc-status/grpc-message); valid after
    /// finish()/recvMessage()==null, owned by the call until deinit.
    pub fn trailers(self: *const RawCall) Metadata {
        return self.resp_trailers;
    }

    /// Consumes one h2 event and advances the call state machine.
    fn step(self: *RawCall) !void {
        var scratch = std.heap.ArenaAllocator.init(self.chan.gpa);
        defer scratch.deinit();
        const ev = self.stream.readEvent(scratch.allocator()) catch |e| switch (e) {
            error.EndOfStream => {
                if (self.stat == null) self.stat = .{ .code = .internal, .message = "stream ended without grpc-status" };
                self.state = .done;
                return;
            },
            error.ConnectionClosed => {
                if (self.stat == null) self.stat = .{ .code = .unavailable, .message = "connection closed" };
                self.state = .done;
                return e;
            },
            // Local cancel() (or a peer RST raced with it) woke a blocked
            // readEvent. Surface it as a cancelled status, not a raw error.
            error.StreamCancelled => {
                if (self.stat == null) self.stat = .{ .code = .cancelled, .message = "cancelled" };
                self.state = .done;
                return;
            },
            else => return e,
        };
        switch (ev) {
            .headers => |hd| try self.onHeaders(hd.headers, hd.end_stream),
            .data => |d| {
                try self.assembler.feed(d.payload);
                if (d.end_stream) {
                    // A gRPC response must end with trailers; DATA+END_STREAM is a violation.
                    if (self.stat == null) self.stat = .{ .code = .internal, .message = "stream ended without trailers" };
                    self.state = .done;
                }
            },
            .rst => |r| {
                if (self.stat == null) self.stat = .{
                    .code = status_mod.codeFromH2Error(r.code),
                    .message = "stream reset by server",
                };
                self.state = .done;
            },
            .goaway => {
                if (self.stat == null) self.stat = .{
                    .code = .unavailable,
                    .message = "stream refused by server GOAWAY (safe to retry)",
                };
                self.state = .done;
            },
        }
    }

    fn onHeaders(self: *RawCall, hs: []const h2.hpack.Header, end_stream: bool) !void {
        const arena = self.arena_state.allocator();
        switch (self.state) {
            .awaiting_headers => {
                if (end_stream) {
                    // Trailers-Only: the whole response is this one HEADERS block.
                    self.stat = try parseTrailerStatus(arena, hs, true);
                    self.resp_trailers = try dupEntries(arena, hs);
                    self.state = .done;
                    return;
                }
                const http_status = findHeader(hs, ":status") orelse "";
                if (!std.mem.eql(u8, http_status, "200")) {
                    const parsed = std.fmt.parseInt(u16, http_status, 10) catch 0;
                    self.stat = .{
                        .code = status_mod.codeFromHttpStatus(parsed),
                        .message = try std.fmt.allocPrint(arena, "unexpected HTTP status \"{s}\"", .{http_status}),
                    };
                    self.abandon();
                    return;
                }
                const ct = findHeader(hs, "content-type") orelse "";
                if (!std.mem.startsWith(u8, ct, "application/grpc")) {
                    self.stat = .{
                        .code = .internal,
                        .message = try std.fmt.allocPrint(arena, "bad content-type \"{s}\"", .{ct}),
                    };
                    self.abandon();
                    return;
                }
                if (findHeader(hs, "grpc-encoding")) |enc| {
                    if (!std.mem.eql(u8, enc, "identity")) {
                        self.stat = .{
                            .code = .internal,
                            .message = try std.fmt.allocPrint(arena, "unsupported grpc-encoding \"{s}\"", .{enc}),
                        };
                        self.abandon();
                        return;
                    }
                }
                self.resp_headers = try dupEntries(arena, hs);
                self.state = .open;
            },
            .open => {
                self.stat = try parseTrailerStatus(arena, hs, false);
                self.resp_trailers = try dupEntries(arena, hs);
                self.state = .done;
            },
            .done => {},
        }
    }

    /// Cancels the underlying stream and ends the call (used on protocol
    /// violations so the server stops sending).
    fn abandon(self: *RawCall) void {
        self.stream.cancel() catch {};
        self.state = .done;
    }
};

fn findHeader(hs: []const h2.hpack.Header, name: []const u8) ?[]const u8 {
    for (hs) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
    }
    return null;
}

/// Copies non-pseudo headers (minus grpc-status/grpc-message) into `arena`.
fn dupEntries(arena: std.mem.Allocator, hs: []const h2.hpack.Header) !Metadata {
    var list: std.ArrayList(Metadata.Entry) = .empty;
    for (hs) |h| {
        if (h.name.len == 0 or h.name[0] == ':') continue;
        if (std.ascii.eqlIgnoreCase(h.name, "grpc-status")) continue;
        if (std.ascii.eqlIgnoreCase(h.name, "grpc-message")) continue;
        try list.append(arena, .{
            .name = try arena.dupe(u8, h.name),
            .value = try arena.dupe(u8, h.value),
        });
    }
    return .{ .entries = list.items };
}

fn parseTrailerStatus(arena: std.mem.Allocator, hs: []const h2.hpack.Header, trailers_only: bool) !Status {
    const msg: []const u8 = if (findHeader(hs, "grpc-message")) |m|
        try status_mod.percentDecode(arena, m)
    else
        "";
    if (findHeader(hs, "grpc-status")) |gs| {
        return .{ .code = status_mod.codeFromGrpcStatus(gs), .message = msg };
    }
    if (trailers_only) {
        const hsv = findHeader(hs, ":status") orelse "";
        const parsed = std.fmt.parseInt(u16, hsv, 10) catch 0;
        return .{ .code = status_mod.codeFromHttpStatus(parsed), .message = "missing grpc-status" };
    }
    return .{ .code = .internal, .message = "missing grpc-status in trailers" };
}

/// Typed wrapper over RawCall for a comptime Method value. Same lifecycle
/// and threading rules as RawCall.
pub fn Call(comptime M: anytype) type {
    const Req = @TypeOf(M).Req;
    const Res = @TypeOf(M).Res;
    return struct {
        raw: RawCall,

        const Self = @This();

        pub fn send(self: *Self, msg: Req) !void {
            const bytes = try M.encode_req(self.raw.chan.gpa, msg);
            defer self.raw.chan.gpa.free(bytes);
            try self.raw.sendMessage(bytes);
        }

        pub fn recv(self: *Self, arena: std.mem.Allocator) !?Res {
            const bytes = (try self.raw.recvMessage(arena)) orelse return null;
            return try M.decode_res(arena, bytes);
        }

        pub fn closeSend(self: *Self) !void {
            return self.raw.closeSend();
        }

        pub fn header(self: *Self) !Metadata {
            return self.raw.header();
        }

        pub fn finish(self: *Self) !Status {
            return self.raw.finish();
        }

        pub fn trailers(self: *const Self) Metadata {
            return self.raw.trailers();
        }

        pub fn cancel(self: *Self) void {
            self.raw.cancel();
        }

        pub fn deinit(self: *Self) void {
            self.raw.deinit();
        }
    };
}

// ---- Task 7: receive state machine tests ----

// Hand-write gRPC message frames (server side). Prefix and payload go in two
// separate writes — each write is an independent DATA frame, which also
// exercises the client's cross-DATA reassembly.
fn writeMsgFrames(res: anytype, payload: []const u8) !void {
    var prefix: [5]u8 = .{ 0, 0, 0, 0, 0 };
    std.mem.writeInt(u32, prefix[1..5], @intCast(payload.len), .big);
    try res.write(&prefix);
    try res.write(payload);
}

fn readAllBody(ctx: *h2.Context, buf: []u8) !usize {
    var n: usize = 0;
    if (ctx.body_reader) |br| {
        while (true) {
            const r = try br.read(buf[n..]);
            if (r == 0) break;
            n += r;
        }
    }
    return n;
}

fn unaryEchoHandler(ctx: *h2.Context) anyerror!void {
    var buf: [256]u8 = undefined;
    const n = try readAllBody(ctx, &buf);
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try ctx.res.header("x-server", "loopback");
    try ctx.res.write(buf[0..n]); // echo verbatim (already includes the 5-byte prefix)
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.trailer("x-trailer", "tv");
    try ctx.res.finish();
}

fn serverStream3Handler(ctx: *h2.Context) anyerror!void {
    var buf: [256]u8 = undefined;
    _ = try readAllBody(ctx, &buf);
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try writeMsgFrames(ctx.res, "m0");
    try writeMsgFrames(ctx.res, "m1");
    try writeMsgFrames(ctx.res, "m2");
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
}

fn notFoundHandler(ctx: *h2.Context) anyerror!void {
    var buf: [256]u8 = undefined;
    _ = try readAllBody(ctx, &buf);
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try ctx.res.trailer("grpc-status", "5");
    try ctx.res.trailer("grpc-message", "no%20such%20thing");
    try ctx.res.finish();
}

test "unary round-trip: message, response headers, trailers, OK status" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, unaryEchoHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var call = try lb.chan.startRaw("/test.Svc/Echo", .{});
    defer call.deinit();
    try call.sendMessage("hello");
    try call.closeSend();

    const hdrs = try call.header();
    try testing.expectEqualStrings("loopback", hdrs.get("x-server").?);

    const msg = (try call.recvMessage(arena)).?;
    try testing.expectEqualStrings("hello", msg);
    try testing.expect((try call.recvMessage(arena)) == null);

    const st = try call.finish();
    try testing.expect(st.isOk());
    try testing.expectEqualStrings("tv", call.trailers().get("x-trailer").?);
}

test "server streaming: three messages split across DATA frames" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, serverStream3Handler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var call = try lb.chan.startRaw("/test.Svc/Stream", .{});
    defer call.deinit();
    try call.sendMessage("req");
    try call.closeSend();

    var i: usize = 0;
    while (try call.recvMessage(arena)) |m| : (i += 1) {
        var expect_buf: [2]u8 = .{ 'm', '0' + @as(u8, @intCast(i)) };
        try testing.expectEqualStrings(&expect_buf, m);
    }
    try testing.expectEqual(@as(usize, 3), i);
    try testing.expect((try call.finish()).isOk());
}

test "non-OK status with percent-decoded message" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, notFoundHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();

    var call = try lb.chan.startRaw("/test.Svc/Nope", .{});
    defer call.deinit();
    try call.sendMessage("x");
    try call.closeSend();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    const st = try call.finish();
    try testing.expectEqual(status_mod.Code.not_found, st.code);
    try testing.expectEqualStrings("no such thing", st.message);
}

// ---- Task 8: abnormal-path tests (raw-frame peer) ----

test "RST_STREAM(CANCEL) maps to cancelled status" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();

    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;
    try rp.writeRst(sid, .cancel);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    // Event order: .rst event → state=done → recvMessage returns null (stream ended).
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    const st = try call.finish();
    try testing.expectEqual(status_mod.Code.cancelled, st.code);
}

test "true Trailers-Only: single HEADERS with END_STREAM carries the status" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();

    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try blk.literal(testing.allocator, "grpc-status", "12");
    try blk.literal(testing.allocator, "grpc-message", "unimplemented");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, blk.buf.items);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    const st = try call.finish();
    try testing.expectEqual(status_mod.Code.unimplemented, st.code);
    try testing.expectEqualStrings("unimplemented", st.message);
    // header() returns an empty Metadata under Trailers-Only.
    try testing.expectEqual(@as(usize, 0), (try call.header()).entries.len);
}

test "trailers missing grpc-status map to internal" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();

    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    // Initial HEADERS (200 + grpc content-type, no END_STREAM).
    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);
    // trailers without grpc-status.
    var tblk: testutil.RawPeer.HpackBlock = .{};
    defer tblk.deinit(testing.allocator);
    try tblk.literal(testing.allocator, "x-oops", "1");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, tblk.buf.items);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    try testing.expectEqual(status_mod.Code.internal, (try call.finish()).code);
}

test "non-grpc content-type maps to internal and cancels the stream" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();

    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "text/html");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    try testing.expectEqual(status_mod.Code.internal, (try call.finish()).code);
    // The client should emit RST_STREAM (abandon).
    const rst = try rp.readUntil(.rst_stream);
    testing.allocator.free(rst.payload);
}

test "compressed-flag message errors and surfaces internal status" {
    var lb: testutil.Loopback = undefined;
    const lbh = struct {
        fn h(ctx: *h2.Context) anyerror!void {
            var buf: [64]u8 = undefined;
            _ = try readAllBody(ctx, &buf);
            ctx.res.status(200);
            try ctx.res.header("content-type", "application/grpc");
            try ctx.res.write(&.{ 1, 0, 0, 0, 0 }); // compressed flag = 1
            try ctx.res.trailer("grpc-status", "0");
            try ctx.res.finish();
        }
    };
    try lb.start(testing.io, testing.allocator, lbh.h, null, .{});
    defer lb.stop();

    var call = try lb.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.sendMessage("x");
    try call.closeSend();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expectError(error.CompressedUnsupported, call.recvMessage(arena_state.allocator()));
    try testing.expectEqual(status_mod.Code.internal, (try call.finish()).code);
}

// ---- Task 9: typed Call(M) / start / unary tests ----

const codec_mod = @import("codec.zig");
const TestMsg = struct {
    text: []const u8 = "",

    pub fn encode(self: TestMsg, gpa: std.mem.Allocator) ![]u8 {
        return gpa.dupe(u8, self.text);
    }

    pub fn decode(arena: std.mem.Allocator, bytes: []const u8) !TestMsg {
        return .{ .text = try arena.dupe(u8, bytes) };
    }
};
const EchoM = codec_mod.Method(TestMsg, TestMsg){ .path = "/test.Svc/Echo" };

test "typed unary convenience round-trips" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, unaryEchoHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const reply = try lb.chan.unary(EchoM, arena_state.allocator(), .{ .text = "ping" }, .{});
    try testing.expectEqualStrings("ping", reply.text);
}

test "typed unary surfaces non-OK via error.RpcFailed and status_out" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, notFoundHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var st: Status = undefined;
    try testing.expectError(error.RpcFailed, lb.chan.unary(
        EchoM,
        arena_state.allocator(),
        .{ .text = "x" },
        .{ .status_out = &st },
    ));
    try testing.expectEqual(status_mod.Code.not_found, st.code);
    try testing.expectEqualStrings("no such thing", st.message);
}

fn bidiEchoHandler(ctx: *h2.Context) anyerror!void {
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    if (ctx.body_reader) |br| {
        var tmp: [512]u8 = undefined;
        while (true) {
            const n = try br.read(&tmp);
            if (n == 0) break;
            try ctx.res.write(tmp[0..n]); // read-and-echo: full-duplex ping-pong
        }
    }
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
}

test "typed bidi ping-pong on one stream" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, bidiEchoHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var call = try lb.chan.start(EchoM, .{});
    defer call.deinit();

    try call.send(.{ .text = "one" });
    try testing.expectEqualStrings("one", (try call.recv(arena)).?.text);
    try call.send(.{ .text = "two" });
    try testing.expectEqualStrings("two", (try call.recv(arena)).?.text);
    try call.closeSend();
    try testing.expect((try call.recv(arena)) == null);
    try testing.expect((try call.finish()).isOk());
}
