//! Channel: one HTTP/2 connection multiplexing concurrent gRPC calls.
//! Transport-agnostic core (`init` over any reader/writer); `connectTcp`
//! (Task 10) adds the h2c dial convenience.

const std = @import("std");
const h2 = @import("zig_http2");
const call_mod = @import("call.zig");
const metadata_mod = @import("metadata.zig");
const Io = std.Io;

pub const Channel = struct {
    io: Io,
    gpa: std.mem.Allocator,
    h2c: h2.Client,
    opts: Options,
    owned: ?*OwnedConn = null,
    authority_owned: bool = false,
    /// RawCalls handed out and not yet deinit'd. `deinit` checks this is zero:
    /// `h2.Client.deinit` frees every `*h2.Stream`, so a call outliving its
    /// channel holds a dangling pointer and its own `deinit` (`stream.close()`)
    /// is a use-after-free. Nothing in this API makes that ordering visible, so
    /// the check turns a silent heap corruption into an immediate, named abort.
    live_calls: std.atomic.Value(u32) = .init(0),

    pub const Options = struct {
        authority: []const u8 = "",
        scheme: []const u8 = "http",
        user_agent: []const u8 = "grpc-zig/0.1",
        max_recv_message_size: u32 = 4 << 20, // grpc-go default
        max_send_message_size: u32 = std.math.maxInt(u32),
    };

    /// `self` must stay at a stable address until `deinit` (the h2 client's
    /// reader thread holds pointers into it). Caller owns `r`/`w`.
    ///
    /// ⚠️ The caller must also **close the transport around `deinit`**. The h2
    /// client's reader thread sits in a blocking read on `r`; `deinit` joins it,
    /// and only EOF/error on the transport gets it out — so deinit'ing a channel
    /// over a still-open connection hangs. `connectTcp` owns its socket and
    /// handles this itself; `init` callers must do it.
    pub fn init(self: *Channel, io: Io, gpa: std.mem.Allocator, r: *Io.Reader, w: *Io.Writer, opts: Options) !void {
        self.* = .{ .io = io, .gpa = gpa, .h2c = undefined, .opts = opts };
        try self.h2c.init(io, gpa, r, w);
    }

    /// Heap-pinned transport state for connectTcp: the reader/writer hold
    /// pointers into these buffers, so the block must never move.
    pub const OwnedConn = struct {
        stream: Io.net.Stream,
        rbuf: [8192]u8 = undefined,
        wbuf: [8192]u8 = undefined,
        sr: Io.net.Stream.Reader = undefined,
        sw: Io.net.Stream.Writer = undefined,
    };

    /// Releases the channel. Every `RawCall`/`Call` opened on it must be
    /// deinit'd FIRST — see `live_calls`. For `init` channels the caller must
    /// also close the transport around this call, or the reader-thread join
    /// blocks forever.
    pub fn deinit(self: *Channel) void {
        // A call still holding an h2 stream would be left dangling by
        // h2c.deinit() below. Checked only where runtime safety is on: the cost
        // is one atomic load, and in a release build we would rather leak the
        // call's arena than add a new panic to a shutdown path.
        if (std.debug.runtime_safety and self.live_calls.load(.acquire) != 0) {
            @panic("zig-grpc: Channel.deinit() with live calls — deinit every RawCall/Call first " ++
                "(h2.Client.deinit frees the streams they point at)");
        }
        // 先 shutdown 自有传输（connectTcp 的 socket），让 h2 reader 线程阻塞的 I/O 读收到
        // 干净 EOF（而非 close 的 EBADF——后者在 io 后端会 panic "programmer bug"）、退出；
        // 否则下面 h2c.deinit 的 reader_thread.join 会挂（持久 HTTP/2 server 是常态——
        // reader 阻塞在 readFrameHeader，dead+broadcast 唤不醒 I/O 读）。
        if (self.owned) |oc| oc.stream.shutdown(self.io, .both) catch {};
        self.h2c.deinit();
        if (self.owned) |oc| {
            oc.stream.close(self.io);
            self.gpa.destroy(oc);
        }
        if (self.authority_owned) self.gpa.free(self.opts.authority);
    }

    /// Dials plaintext h2c TCP (prior knowledge) and initializes the channel.
    /// `host` may be an IP literal or a hostname. The channel owns the socket.
    /// For TLS, wrap your own reader/writer and use `init` instead.
    pub fn connectTcp(self: *Channel, io: Io, gpa: std.mem.Allocator, host: []const u8, port: u16, opts: Options) !void {
        const oc = try gpa.create(OwnedConn);
        errdefer gpa.destroy(oc);
        oc.* = .{ .stream = undefined };
        if (Io.net.IpAddress.parse(host, port)) |a| {
            var addr = a;
            oc.stream = try addr.connect(io, .{ .mode = .stream });
        } else |_| {
            const hn = try Io.net.HostName.init(host);
            oc.stream = try hn.connect(io, port, .{ .mode = .stream });
        }
        errdefer oc.stream.close(io);
        oc.sr = oc.stream.reader(io, &oc.rbuf);
        oc.sw = oc.stream.writer(io, &oc.wbuf);

        var o = opts;
        var auth_owned = false;
        if (o.authority.len == 0) {
            o.authority = try std.fmt.allocPrint(gpa, "{s}:{d}", .{ host, port });
            auth_owned = true;
        }
        errdefer if (auth_owned) gpa.free(o.authority);

        try self.init(io, gpa, &oc.sr.interface, &oc.sw.interface, o);
        self.owned = oc;
        self.authority_owned = auth_owned;
    }

    /// Opens a bytes-level call: sends request HEADERS, returns the RawCall.
    /// The returned value owns per-call state; call `deinit` when done.
    pub fn startRaw(self: *Channel, path: []const u8, call_opts: call_mod.CallOptions) !call_mod.RawCall {
        // RequestHead.headers is []const hpack.Header (not the types.Header
        // re-exported as h2.Header); they are structurally identical but
        // distinct named types, so build the one openStream consumes.
        var headers: std.ArrayList(h2.hpack.Header) = .empty;
        defer headers.deinit(self.gpa);
        try headers.append(self.gpa, .{ .name = "te", .value = "trailers" });
        try headers.append(self.gpa, .{ .name = "content-type", .value = "application/grpc" });
        try headers.append(self.gpa, .{ .name = "user-agent", .value = self.opts.user_agent });
        var tbuf: [9]u8 = undefined;
        if (call_opts.timeout_ns) |ns| {
            try headers.append(self.gpa, .{ .name = "grpc-timeout", .value = metadata_mod.encodeTimeout(ns, &tbuf) });
        }
        for (call_opts.metadata) |e| {
            if (metadata_mod.isReservedName(e.name)) return error.ReservedMetadataName;
            if (!metadata_mod.isValidName(e.name)) return error.InvalidMetadataName;
            if (!metadata_mod.isValidValue(e.value)) return error.InvalidMetadataValue;
            try headers.append(self.gpa, .{ .name = e.name, .value = e.value });
        }
        const s = try self.h2c.openStream(.{
            .method = "POST",
            .scheme = self.opts.scheme,
            .path = path,
            .authority = self.opts.authority,
            .headers = headers.items,
        }, false);
        return call_mod.RawCall.init(self, s);
    }

    /// Opens a typed call for a comptime Method value.
    pub fn start(self: *Channel, comptime M: anytype, call_opts: call_mod.CallOptions) !call_mod.Call(M) {
        return .{ .raw = try self.startRaw(M.path, call_opts) };
    }

    /// One-shot unary RPC: send → half-close → receive one → status check.
    /// Non-OK becomes error.RpcFailed with the status (message duped into
    /// `arena`) written to `call_opts.status_out` when provided.
    pub fn unary(
        self: *Channel,
        comptime M: anytype,
        arena: std.mem.Allocator,
        req: @TypeOf(M).Req,
        call_opts: call_mod.CallOptions,
    ) !@TypeOf(M).Res {
        var c = try self.start(M, call_opts);
        defer c.deinit();
        try c.send(req);
        try c.closeSend();
        const res = try c.recv(arena);
        const st = try c.finish();
        if (call_opts.status_out) |out| {
            out.* = .{ .code = st.code, .message = try arena.dupe(u8, st.message) };
        }
        if (!st.isOk()) return error.RpcFailed;
        return res orelse error.MissingResponse;
    }
};

const testing = std.testing;
const testutil = @import("testutil.zig");

const Captured = struct {
    ok: std.atomic.Value(bool) = .init(false),
    saw_te_trailers: bool = false,
    saw_content_type: bool = false,
    saw_user_agent: bool = false,
    saw_custom_md: bool = false,
    saw_timeout: bool = false,
    body: [64]u8 = undefined,
    body_len: usize = 0,
};

fn captureHandler(ctx: *h2.Context) anyerror!void {
    const cap: *Captured = @ptrCast(@alignCast(ctx.userdata.?));
    cap.saw_te_trailers = if (ctx.req.get("te")) |v| std.mem.eql(u8, v, "trailers") else false;
    cap.saw_content_type = if (ctx.req.get("content-type")) |v| std.mem.eql(u8, v, "application/grpc") else false;
    cap.saw_user_agent = if (ctx.req.get("user-agent")) |v| std.mem.startsWith(u8, v, "grpc-zig/") else false;
    cap.saw_custom_md = if (ctx.req.get("x-trace-id")) |v| std.mem.eql(u8, v, "t1") else false;
    cap.saw_timeout = ctx.req.get("grpc-timeout") != null;
    if (ctx.body_reader) |br| {
        while (true) {
            const n = try br.read(cap.body[cap.body_len..]);
            if (n == 0) break;
            cap.body_len += n;
        }
    }
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
    cap.ok.store(true, .release);
}

test "startRaw sends gRPC request headers and framed messages" {
    var cap: Captured = .{};
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, captureHandler, &cap, .{});

    var call = try lb.chan.startRaw("/test.Svc/Echo", .{
        .metadata = &.{.{ .name = "x-trace-id", .value = "t1" }},
        .timeout_ns = 3 * std.time.ns_per_s,
    });
    try call.sendMessage("hi");
    try call.closeSend();
    // Wait for the handler to finish (send path has no recv API until Task 7).
    while (!cap.ok.load(.acquire)) {
        std.Io.sleep(testing.io, .{ .nanoseconds = 5 * std.time.ns_per_ms }, .awake) catch {};
    }
    call.deinit();
    lb.stop();

    try testing.expect(cap.saw_te_trailers);
    try testing.expect(cap.saw_content_type);
    try testing.expect(cap.saw_user_agent);
    try testing.expect(cap.saw_custom_md);
    try testing.expect(cap.saw_timeout);
    // 5-byte prefix (flag 0, len 2) + "hi"
    try testing.expectEqual(@as(usize, 7), cap.body_len);
    try testing.expectEqualSlices(u8, &.{ 0, 0, 0, 0, 2 }, cap.body[0..5]);
    try testing.expectEqualStrings("hi", cap.body[5..7]);
}

test "reserved metadata names are rejected" {
    var cap: Captured = .{};
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, captureHandler, &cap, .{});
    defer lb.stop();
    try testing.expectError(error.ReservedMetadataName, lb.chan.startRaw("/x", .{
        .metadata = &.{.{ .name = "grpc-timeout", .value = "1S" }},
    }));
}

// ---- Task 10: connectTcp test ----

fn tcpOkHandler(ctx: *h2.Context) anyerror!void {
    if (ctx.body_reader) |br| {
        var tmp: [64]u8 = undefined;
        while (true) {
            if (try br.read(&tmp) == 0) break;
        }
    }
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
}

const TcpSrv = struct {
    io: std.Io,
    listener: *std.Io.net.Server,
    srv: *h2.Server,

    fn run(self: *TcpSrv) void {
        var accepted = self.listener.accept(self.io) catch return;
        defer accepted.close(self.io);
        var rbuf: [8192]u8 = undefined;
        var wbuf: [8192]u8 = undefined;
        var sr = accepted.reader(self.io, &rbuf);
        var sw = accepted.writer(self.io, &wbuf);
        h2.serveConn(self.srv, &sr.interface, &sw.interface, null, "http");
    }
};

test "connectTcp dials h2c and completes a call" {
    const io = testing.io;
    const addr0 = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try addr0.listen(io, .{ .mode = .stream, .reuse_address = true });
    defer listener.deinit(io);
    const port = listener.socket.address.ip4.port;

    var srv: h2.Server = .{ .io = io, .gpa = testing.allocator, .handler = tcpOkHandler };
    var tsrv: TcpSrv = .{ .io = io, .listener = &listener, .srv = &srv };
    const th = try std.Thread.spawn(.{}, TcpSrv.run, .{&tsrv});

    var chan: Channel = undefined;
    try chan.connectTcp(io, testing.allocator, "127.0.0.1", port, .{});

    var call = try chan.startRaw("/test.Svc/Ok", .{});
    try call.sendMessage("x");
    try call.closeSend();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
    try testing.expect((try call.finish()).isOk());
    // authority auto-filled
    try testing.expect(std.mem.startsWith(u8, chan.opts.authority, "127.0.0.1:"));
    call.deinit();
    chan.deinit(); // close socket → server reads EOF → thread exits
    th.join();
}

test "metadata with control bytes is rejected before it reaches the wire" {
    var cap: Captured = .{};
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, captureHandler, &cap, .{});
    defer lb.stop();

    try testing.expectError(error.InvalidMetadataValue, lb.chan.startRaw("/x", .{
        .metadata = &.{.{ .name = "x-trace-id", .value = "ok\r\nx-injected: 1" }},
    }));
    try testing.expectError(error.InvalidMetadataName, lb.chan.startRaw("/x", .{
        .metadata = &.{.{ .name = "bad name", .value = "v" }},
    }));
}

test "live_calls tracks outstanding calls so deinit can refuse to strand one" {
    var cap: Captured = .{};
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, captureHandler, &cap, .{});
    defer lb.stop();

    try testing.expectEqual(@as(u32, 0), lb.chan.live_calls.load(.acquire));
    var a = try lb.chan.startRaw("/test.Svc/A", .{});
    var b = try lb.chan.startRaw("/test.Svc/B", .{});
    try testing.expectEqual(@as(u32, 2), lb.chan.live_calls.load(.acquire));
    a.deinit();
    try testing.expectEqual(@as(u32, 1), lb.chan.live_calls.load(.acquire));
    b.deinit();
    // Back to zero, so lb.stop()'s chan.deinit() passes its check.
    try testing.expectEqual(@as(u32, 0), lb.chan.live_calls.load(.acquire));
}
