//! Per-call state. RawCall drives one h2 stream through the gRPC call
//! lifecycle; Call(M) (Task 9) is the typed wrapper on top.

const std = @import("std");
const h2 = @import("zig_http2");
const status_mod = @import("status.zig");
const metadata_mod = @import("metadata.zig");
const frame = @import("frame.zig");
const channel_mod = @import("channel.zig");
const Io = std.Io;

const testing = std.testing;
const testutil = @import("testutil.zig");

pub const Status = status_mod.Status;
pub const Metadata = metadata_mod.Metadata;

pub const CallOptions = struct {
    metadata: []const Metadata.Entry = &.{},
    /// Encoded as `grpc-timeout`; enforced by the SERVER. v1 has no local
    /// deadline timer — to abort a blocked recv early, call `cancel()` from
    /// another thread.
    timeout_ns: ?u64 = null,
    /// For `Channel.unary` only: receives the call's final Status — on success,
    /// on a non-OK status, and on a transport failure that never produced one
    /// (mapped via `status.codeFromTransportError`). The message is duped into
    /// the arena passed to `unary`.
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
    /// Serializes the receive path. `recvMessage`, `header` and `finish` all
    /// drive `step()`, which mutates `state`/`stat`/`resp_headers`/`arena_state`
    /// /`assembler` with no other protection — zig-http2 makes `readEvent`
    /// thread-safe per stream, but that protects the transport, not this call.
    /// `cancel()` deliberately stays outside: it touches only the h2 stream, so
    /// it can still break a receiver out of a blocking read.
    recv_mu: Io.Mutex = .init,
    /// Set once the response-head phase has a verdict — initial HEADERS
    /// decoded, or the stream ended some other way. Lets `header()` answer
    /// without taking `recv_mu`; see the fast path there.
    head_settled: std.atomic.Value(bool) = .init(false),
    /// Waited on by a `header()` that found another thread already driving the
    /// receive path. It must NOT wait on `recv_mu`: a receiver keeps that lock
    /// across a blocking `readEvent`, so once it has consumed the HEADERS it
    /// goes straight back to waiting for the next message, still holding it —
    /// and `header()` would be stuck behind a value that already arrived.
    head_mu: Io.Mutex = .init,
    head_cond: Io.Condition = .init,

    const State = enum { awaiting_headers, open, done };

    pub fn init(chan: *channel_mod.Channel, stream: *h2.Stream) RawCall {
        // Paired with the decrement in `deinit`; `Channel.deinit` refuses to run
        // while any call is outstanding, because it frees the streams they hold.
        _ = chan.live_calls.fetchAdd(1, .monotonic);
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
        try self.stream.send("", true);
        self.send_closed = true; // 置位在 send 之后：失败则可重试（否则 server 永远等不到 END_STREAM）
    }

    /// Tells the server to cancel (RST_STREAM CANCEL). Safe to call from a
    /// thread other than the receiver: the underlying stream wakes a blocked
    /// `recvMessage`/`header`, which returns promptly with a `cancelled` status.
    ///
    /// Deliberately touches no `RawCall` state — `stat`/`state` are owned by the
    /// receiver thread and are materialized there when `step()` observes the
    /// resulting `error.StreamCancelled`, so cancel never races the receiver.
    pub fn cancel(self: *RawCall) void {
        self.stream.cancel() catch {};
    }

    /// Releases the call. Must not race a concurrent sendMessage/recvMessage,
    /// and must happen before the owning `Channel` is deinit'd — the channel
    /// frees the underlying h2 stream.
    pub fn deinit(self: *RawCall) void {
        self.stream.close();
        self.assembler.deinit();
        self.arena_state.deinit();
        _ = self.chan.live_calls.fetchSub(1, .monotonic);
    }

    /// Reads the next response message into `arena`. Returns null once the
    /// server has finished (trailers received) — then `finish()` has the
    /// status. Transport faults are Zig errors; `stat` still carries the
    /// mapped gRPC status afterwards.
    ///
    /// Serialized against `header()`/`finish()` via `recv_mu`, so calling them
    /// from different threads is safe; it is still one *logical* receiver
    /// (whoever gets the lock consumes the next event).
    pub fn recvMessage(self: *RawCall, arena: std.mem.Allocator) !?[]u8 {
        self.recv_mu.lockUncancelable(self.chan.io);
        defer self.recv_mu.unlock(self.chan.io);
        return self.recvLocked(arena);
    }

    /// `recv_mu` held. `finish()` reuses this instead of `recvMessage` because
    /// `Io.Mutex` is not reentrant.
    fn recvLocked(self: *RawCall, arena: std.mem.Allocator) !?[]u8 {
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
    /// Safe to call from a thread other than the one in `recvMessage`.
    pub fn header(self: *RawCall) !Metadata {
        while (true) {
            // Already settled: `resp_headers` is frozen (later HEADERS land in
            // `resp_trailers`), so answer without touching a lock.
            if (self.head_settled.load(.acquire)) return self.resp_headers orelse .{};

            // Nobody is driving the receive path — drive it ourselves, far
            // enough to get the head phase decided.
            if (self.recv_mu.tryLock()) {
                defer self.recv_mu.unlock(self.chan.io);
                // Release any waiters even when we leave by error: an
                // unclassified transport error can return with `state` still
                // `.awaiting_headers`, in which case step()'s own wakeup does
                // not fire and they would wait forever. They re-check and take
                // over from the top of the loop.
                defer self.wakeHeadWaiters();
                while (self.resp_headers == null and self.state == .awaiting_headers) try self.step();
                return self.resp_headers orelse .{};
            }

            // Someone else is driving. Wait for the head phase to be *decided*,
            // not for the lock they are holding.
            self.head_mu.lockUncancelable(self.chan.io);
            if (!self.head_settled.load(.acquire)) self.head_cond.waitUncancelable(self.chan.io, &self.head_mu);
            self.head_mu.unlock(self.chan.io);
            // Loop: either it settled, or the driver bailed and we take over.
        }
    }

    /// Wakes threads parked in `header()`. Takes `head_mu` so a wakeup cannot
    /// slip between a waiter's `head_settled` re-check and its `wait`.
    fn wakeHeadWaiters(self: *RawCall) void {
        self.head_mu.lockUncancelable(self.chan.io);
        self.head_cond.broadcast(self.chan.io);
        self.head_mu.unlock(self.chan.io);
    }

    /// Drains any remaining messages, then returns the final status. Intended
    /// after recvMessage returned null (for streams) or directly (unary
    /// convenience). NOTE: blocks until the server ends the stream.
    pub fn finish(self: *RawCall) !Status {
        self.recv_mu.lockUncancelable(self.chan.io);
        defer self.recv_mu.unlock(self.chan.io);
        var scratch = std.heap.ArenaAllocator.init(self.chan.gpa);
        defer scratch.deinit();
        while (self.state != .done) {
            _ = self.recvLocked(scratch.allocator()) catch break;
            _ = scratch.reset(.retain_capacity);
        }
        return self.stat orelse .{ .code = .internal, .message = "call ended without status" };
    }

    /// Trailer metadata (excluding grpc-status/grpc-message); valid after
    /// finish()/recvMessage()==null, owned by the call until deinit.
    pub fn trailers(self: *const RawCall) Metadata {
        return self.resp_trailers;
    }

    /// Consumes one h2 event and advances the call state machine. `recv_mu` held.
    fn step(self: *RawCall) !void {
        // Publish the head-phase verdict on every exit path (including the
        // error ones). Release pairs with header()'s acquire, so a reader that
        // sees the flag also sees the `resp_headers`/`state` writes behind it.
        // `swap` so the wakeup fires once, on the transition — broadcasting on
        // every later event would be pure overhead on the hot path.
        defer if (self.state != .awaiting_headers and !self.head_settled.swap(true, .release)) self.wakeHeadWaiters();
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
                // A gRPC response opens with HEADERS; DATA before them belongs
                // to no valid response. Buffering it anyway was wrong twice
                // over: the bytes surfaced as a legitimate message, and
                // `header()` drives `step()` WITHOUT calling `assembler.next()`,
                // so `max_recv_message_size` never got a chance to fire and a
                // peer could grow the assembler without bound while we waited
                // for headers that never came.
                if (self.state == .awaiting_headers) {
                    if (self.stat == null) self.stat = .{
                        .code = .internal,
                        .message = "DATA received before response headers",
                    };
                    self.abandon();
                    return;
                }
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

const CancelReceiver = struct {
    call: *RawCall,
    got_null: bool = false,
    done: std.atomic.Value(bool) = .init(false),

    fn run(self: *CancelReceiver) void {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        // A cancel wakes readEvent → StreamCancelled → step() ends the call, so
        // recvMessage returns null (not a Zig error). `catch null` is just belt.
        const m = self.call.recvMessage(arena_state.allocator()) catch null;
        self.got_null = (m == null);
        self.done.store(true, .release);
    }
};

test "cancel() from another thread unblocks recvMessage with cancelled status" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();
    // Drain the client's HEADERS; the peer then stays silent so the receiver
    // thread blocks in readEvent until the cancel wakes it.
    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);

    var recv: CancelReceiver = .{ .call = &call };
    const th = try std.Thread.spawn(.{}, CancelReceiver.run, .{&recv});
    // Give the receiver a moment to block, so we exercise the wake path.
    std.Io.sleep(testing.io, .{ .nanoseconds = 20 * std.time.ns_per_ms }, .awake) catch {};
    call.cancel();
    th.join();

    try testing.expect(recv.got_null);
    try testing.expectEqual(status_mod.Code.cancelled, (try call.finish()).code);
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

test "DATA before response HEADERS is a protocol error, not a message" {
    // Previously the bytes were fed into the assembler and surfaced as a
    // perfectly ordinary message with an OK status. `header()` also drives
    // step() without ever calling assembler.next(), so max_recv_message_size
    // never applied and a peer could grow the buffer without bound.
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();
    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    // A complete, well-formed gRPC message — but sent before any HEADERS.
    const msg = [_]u8{ 0, 0, 0, 0, 4 } ++ "oops".*;
    try rp.writeFrame(.data, 0, sid, &msg);
    // Then a response that would otherwise look entirely normal.
    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);
    var tblk: testutil.RawPeer.HpackBlock = .{};
    defer tblk.deinit(testing.allocator);
    try tblk.literal(testing.allocator, "grpc-status", "0");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, tblk.buf.items);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    // No message is delivered, and the call ends internal rather than OK.
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    const st = try call.finish();
    try testing.expectEqual(status_mod.Code.internal, st.code);
    try testing.expectEqualStrings("DATA received before response headers", st.message);
    // abandon() tells the server to stop.
    const rst = try rp.readUntil(.rst_stream);
    testing.allocator.free(rst.payload);
}

/// Runs the blocking `header()` on its own thread so the test can put a
/// deadline on it: without the DATA-before-HEADERS guard, `header()` never
/// returns (it keeps consuming DATA and waiting for headers that never come),
/// and a test that called it inline would hang the suite instead of failing.
const HeaderWaiter = struct {
    call: *RawCall,
    entries: usize = 0,
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),

    fn run(self: *HeaderWaiter) void {
        if (self.call.header()) |md| {
            self.entries = md.entries.len;
        } else |e| self.err = e;
        self.done.store(true, .release);
    }
};

test "header() ends on DATA-before-HEADERS instead of buffering forever" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();
    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    // The unbounded path: header() loops on step() and never calls
    // assembler.next(), so each of these used to be appended with no size cap.
    var chunk: [1024]u8 = @splat('x');
    var i: usize = 0;
    while (i < 8) : (i += 1) try rp.writeFrame(.data, 0, sid, &chunk);

    var w: HeaderWaiter = .{ .call = &call };
    const th = try std.Thread.spawn(.{}, HeaderWaiter.run, .{&w});
    var finished = false;
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 20) {
        if (w.done.load(.acquire)) {
            finished = true;
            break;
        }
        std.Io.sleep(testing.io, .{ .nanoseconds = 20 * std.time.ns_per_ms }, .awake) catch {};
    }
    // Release the thread either way so join() returns and the failure below is
    // what reports the problem.
    if (!finished) call.cancel();
    th.join();

    try testing.expect(finished);
    try testing.expectEqual(@as(usize, 0), w.entries); // Trailers-Only-shaped empty
    try testing.expectEqual(status_mod.Code.internal, (try call.finish()).code);
    try testing.expect(!call.assembler.hasPartial()); // nothing was buffered
}

/// Blocks in `recvMessage` waiting for a message the peer never sends, so a
/// test can check what another thread's `header()` does while it holds nothing
/// but a parked `readEvent`.
const BlockedRecv = struct {
    call: *RawCall,
    done: std.atomic.Value(bool) = .init(false),

    fn run(self: *BlockedRecv) void {
        var a = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer a.deinit();
        _ = self.call.recvMessage(a.allocator()) catch {};
        self.done.store(true, .release);
    }
};

const HeaderRacer = struct {
    call: *RawCall,
    saw_content_type: bool = false,
    done: std.atomic.Value(bool) = .init(false),

    fn run(self: *HeaderRacer) void {
        if (self.call.header()) |md| {
            self.saw_content_type = md.get("content-type") != null;
        } else |_| {}
        self.done.store(true, .release);
    }
};

test "header() answers while another thread is parked in recvMessage" {
    // Serializing the receive path on one mutex is right, but `header()` must
    // not queue behind a receiver waiting for the *next* message: on a
    // server-streaming call that is an unbounded wait for a value already in
    // hand. The head_settled fast path is what makes this return.
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();
    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    // Response headers only — no message, no trailers.
    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);

    // Receiver consumes those HEADERS and then parks waiting for DATA.
    var recv: BlockedRecv = .{ .call = &call };
    const th_recv = try std.Thread.spawn(.{}, BlockedRecv.run, .{&recv});
    std.Io.sleep(testing.io, .{ .nanoseconds = 100 * std.time.ns_per_ms }, .awake) catch {};

    var racer: HeaderRacer = .{ .call = &call };
    const th_hdr = try std.Thread.spawn(.{}, HeaderRacer.run, .{&racer});
    var header_returned = false;
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 20) {
        if (racer.done.load(.acquire)) {
            header_returned = true;
            break;
        }
        std.Io.sleep(testing.io, .{ .nanoseconds = 20 * std.time.ns_per_ms }, .awake) catch {};
    }
    // Sample before unblocking: the receiver must still be parked, which is
    // what makes this a real test of the fast path rather than of timing.
    const recv_still_parked = !recv.done.load(.acquire);

    // Release everyone: trailers end the stream.
    var tblk: testutil.RawPeer.HpackBlock = .{};
    defer tblk.deinit(testing.allocator);
    try tblk.literal(testing.allocator, "grpc-status", "0");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, tblk.buf.items);
    th_recv.join();
    th_hdr.join();

    try testing.expect(header_returned);
    try testing.expect(recv_still_parked);
    try testing.expect(racer.saw_content_type);
}

test "header() started before HEADERS is not stuck behind a parked receiver" {
    // The slow path, which the fast-path test above does not reach: both
    // threads start while the head phase is still undecided, so header() finds
    // head_settled == false AND the receiver already holding recv_mu. Waiting
    // on the mutex would mean waiting for the first message (or the end of the
    // stream), because the receiver reacquires nothing — it never lets go.
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var call = try rp.chan.startRaw("/test.Svc/X", .{});
    defer call.deinit();
    try call.closeSend();
    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    // Receiver first, with nothing to consume yet: it takes recv_mu and parks.
    var recv: BlockedRecv = .{ .call = &call };
    const th_recv = try std.Thread.spawn(.{}, BlockedRecv.run, .{&recv});
    std.Io.sleep(testing.io, .{ .nanoseconds = 100 * std.time.ns_per_ms }, .awake) catch {};

    // header() now enters with the head phase undecided and the lock taken.
    var racer: HeaderRacer = .{ .call = &call };
    const th_hdr = try std.Thread.spawn(.{}, HeaderRacer.run, .{&racer});
    std.Io.sleep(testing.io, .{ .nanoseconds = 100 * std.time.ns_per_ms }, .awake) catch {};

    // Only now do the headers arrive. The receiver consumes them and goes
    // straight back to waiting for DATA, still holding recv_mu.
    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);

    var header_returned = false;
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 20) {
        if (racer.done.load(.acquire)) {
            header_returned = true;
            break;
        }
        std.Io.sleep(testing.io, .{ .nanoseconds = 20 * std.time.ns_per_ms }, .awake) catch {};
    }
    const recv_still_parked = !recv.done.load(.acquire);

    var tblk: testutil.RawPeer.HpackBlock = .{};
    defer tblk.deinit(testing.allocator);
    try tblk.literal(testing.allocator, "grpc-status", "0");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, tblk.buf.items);
    th_recv.join();
    th_hdr.join();

    try testing.expect(header_returned);
    try testing.expect(recv_still_parked);
    try testing.expect(racer.saw_content_type);
}

test "header() drives the receive path itself when no receiver is running" {
    // The other half of the slow path: nobody holds recv_mu, so header() must
    // take it and step() until the headers land — otherwise waiting on
    // head_cond would hang, since nothing else would ever decide the phase.
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
    try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);

    const md = try call.header();
    try testing.expectEqualStrings("application/grpc", md.get("content-type").?);
}
