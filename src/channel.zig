//! Channel: one HTTP/2 connection multiplexing concurrent gRPC calls.
//! Transport-agnostic core (`init` over any reader/writer); `connectTcp`
//! adds the h2c dial convenience.

const builtin = @import("builtin");
const std = @import("std");
const h2 = @import("zig_http2");
const call_mod = @import("call.zig");
const metadata_mod = @import("metadata.zig");
const status_mod = @import("status.zig");
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
        /// Enables `TCP_NODELAY` on sockets dialed by `connectTcp` so Nagle's
        /// algorithm does not delay small HTTP/2 frames (HEADERS/DATA/END_STREAM
        /// and streamed messages) by up to an RTT. Ignored by `init` — callers
        /// managing their own TCP transport should configure `TCP_NODELAY` on
        /// their socket directly.
        tcp_nodelay: bool = true,
    };

    /// `self` must stay at a stable address until `deinit` (the h2 client's
    /// reader thread holds pointers into it). Caller owns `r`/`w`.
    ///
    /// ⚠️ The caller must also **close the transport around `deinit`**. The h2
    /// client's reader thread sits in a blocking read on `r`; `deinit` joins it,
    /// and only EOF/error on the transport gets it out — so deinit'ing a channel
    /// over a still-open connection hangs. `connectTcp` owns its socket and
    /// handles this itself; `init` callers must do it.
    ///
    /// When `r`/`w` wrap a TCP socket, callers should enable `TCP_NODELAY` on
    /// the socket (as `connectTcp` does by default) to avoid Nagle stalling
    /// request DATA and streamed messages behind unacknowledged HEADERS.
    ///
    /// `opts.authority` is required here. HTTP/2 requests carry `:authority`,
    /// gRPC servers route and enforce TLS host checks on it, and
    /// `hpack.Encoder.encodeRequest` simply omits the pseudo-header when the
    /// value is empty — so defaulting to "" produced requests that a real
    /// server (grpc-go among them) rejects, with nothing here to say why.
    /// `connectTcp` derives `host:port`; over a transport we did not dial, only
    /// the caller knows the authority.
    pub fn init(self: *Channel, io: Io, gpa: std.mem.Allocator, r: *Io.Reader, w: *Io.Writer, opts: Options) !void {
        if (opts.authority.len == 0) return error.AuthorityRequired;
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
    /// `host` may be an IPv4/IPv6 literal (bare or `[...]`-bracketed) or a
    /// hostname. The channel owns the socket. For TLS, wrap your own
    /// reader/writer and use `init` instead.
    pub fn connectTcp(self: *Channel, io: Io, gpa: std.mem.Allocator, host: []const u8, port: u16, opts: Options) !void {
        const was_bracketed = host.len >= 2 and host[0] == '[' and host[host.len - 1] == ']';
        const bare_host = if (was_bracketed) host[1 .. host.len - 1] else host;

        const oc = try gpa.create(OwnedConn);
        errdefer gpa.destroy(oc);
        oc.* = .{ .stream = undefined };
        if (Io.net.IpAddress.parse(bare_host, port)) |a| {
            var addr = a;
            oc.stream = try addr.connect(io, .{ .mode = .stream });
        } else |parse_err| {
            if (was_bracketed) return parse_err;
            const hn = try Io.net.HostName.init(host);
            oc.stream = try hn.connect(io, port, .{ .mode = .stream });
        }
        errdefer oc.stream.close(io);
        if (opts.tcp_nodelay) setTcpNoDelay(oc.stream.socket.handle);
        oc.sr = oc.stream.reader(io, &oc.rbuf);
        oc.sw = oc.stream.writer(io, &oc.wbuf);

        var o = opts;
        var auth_owned = false;
        if (o.authority.len == 0) {
            o.authority = try formatDefaultAuthority(gpa, bare_host, port);
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

    /// One-shot unary RPC: encode → open → send → half-close → receive one →
    /// finish → decode. Non-OK server status becomes `error.RpcFailed` (and
    /// takes precedence even if a received payload cannot be decoded).
    /// `call_opts.status_out`, when provided, always receives the final Status:
    /// transport failures map via `status.codeFromTransportError`, and request
    /// encode / response decode failures map to `.internal` (or
    /// `.resource_exhausted` on `OutOfMemory`).
    pub fn unary(
        self: *Channel,
        comptime M: anytype,
        arena: std.mem.Allocator,
        req: @TypeOf(M).Req,
        call_opts: call_mod.CallOptions,
    ) !@TypeOf(M).Res {
        const req_bytes = M.encode_req(self.gpa, req) catch |e| {
            recordCodecFailure(call_opts, arena, e);
            return e;
        };
        defer self.gpa.free(req_bytes);

        var raw = self.startRaw(M.path, call_opts) catch |e| {
            // No call exists yet, so there is no RawCall.stat to prefer.
            writeStatusBestEffort(call_opts, arena, .{
                .code = status_mod.codeFromTransportError(e),
                .message = @errorName(e),
            });
            return e;
        };
        defer raw.deinit();

        sendUnaryPayload(&raw, req_bytes) catch |e| {
            recordFailure(&raw, call_opts, arena, e);
            return e;
        };
        const raw_msg = recvUnaryPayload(&raw, arena) catch |e| {
            switch (e) {
                error.TooManyResponses => writeStatusBestEffort(call_opts, arena, .{
                    .code = .internal,
                    .message = "unary response carried more than one message",
                }),
                else => recordFailure(&raw, call_opts, arena, e),
            }
            return e;
        };
        const st = raw.finish() catch |e| {
            recordFailure(&raw, call_opts, arena, e);
            return e;
        };

        if (!st.isOk()) {
            writeStatusBestEffort(call_opts, arena, st);
            return error.RpcFailed;
        }
        const msg_bytes = raw_msg orelse {
            // OK trailers but no message: the server broke the unary contract.
            writeStatusBestEffort(call_opts, arena, .{
                .code = .internal,
                .message = "server ended the call without a response message",
            });
            return error.MissingResponse;
        };
        const res = M.decode_res(arena, msg_bytes) catch |e| {
            recordCodecFailure(call_opts, arena, e);
            return e;
        };
        try writeStatus(call_opts, arena, st);
        return res;
    }
};

/// Sends one unary request payload and half-closes the stream.
/// If `sendMessage` or `closeSend` hits `error.StreamReset`, the peer already
/// ended the stream (e.g. early Trailers-Only + RST_STREAM(NO_ERROR), graceful
/// GOAWAY refusal, or RST_STREAM), or a local `cancel()` fired. Swallowing it
/// lets `unary()` fall through to `recv`/`finish` and surface the real Status
/// instead of collapsing to `Code.unknown`.
fn sendUnaryPayload(raw: *call_mod.RawCall, req_bytes: []const u8) !void {
    raw.sendMessage(req_bytes) catch |e| switch (e) {
        error.StreamReset => return,
        else => return e,
    };
    raw.closeSend() catch |e| switch (e) {
        error.StreamReset => return,
        else => return e,
    };
}

/// Receives at most one unary response message at the frame level.
/// When one arrives, probes once more before returning (skipping decode of any
/// illegal extra message): on a well-behaved response this consumes trailers
/// and returns null with no extra round trip; if a second message arrives, it
/// bails with `error.TooManyResponses` so `defer raw.deinit()` resets the
/// stream instead of hanging in `finish()` to drain an unbounded server stream.
fn recvUnaryPayload(raw: *call_mod.RawCall, arena: std.mem.Allocator) !?[]u8 {
    const first = (try raw.recvMessage(arena)) orelse return null;
    if ((try raw.recvMessage(arena)) != null) return error.TooManyResponses;
    return first;
}

/// Best-effort `TCP_NODELAY` on POSIX targets so Nagle does not stall small
/// HTTP/2 frames behind unacknowledged HEADERS.
fn setTcpNoDelay(handle: Io.net.Socket.Handle) void {
    switch (builtin.os.tag) {
        .windows, .wasi => return,
        else => {
            if (!@hasDecl(std.posix.system, "setsockopt") or
                !@hasDecl(std.posix.system, "IPPROTO") or
                !@hasDecl(std.posix.system, "TCP"))
            {
                return;
            }
            const on: c_int = 1;
            std.posix.setsockopt(
                handle,
                std.posix.IPPROTO.TCP,
                std.posix.TCP.NODELAY,
                std.mem.asBytes(&on),
            ) catch {};
        },
    }
}

/// Formats `host:port` for `:authority`, enclosing IPv6 literals in `[...]` per
/// RFC 3986 §3.2.2 / RFC 9113 §8.3.1.
fn formatDefaultAuthority(gpa: std.mem.Allocator, bare_host: []const u8, port: u16) ![]u8 {
    if (std.mem.indexOfScalar(u8, bare_host, ':') != null) {
        return std.fmt.allocPrint(gpa, "[{s}]:{d}", .{ bare_host, port });
    }
    return std.fmt.allocPrint(gpa, "{s}:{d}", .{ bare_host, port });
}

/// Copies `st` into `call_opts.status_out`, duping the message into `arena` —
/// a status read off the call points into the call's arena, which dies with it.
/// Used where the RPC otherwise succeeded, so an allocator failure is reported
/// rather than silently producing a status with no message.
fn writeStatus(call_opts: call_mod.CallOptions, arena: std.mem.Allocator, st: call_mod.Status) !void {
    const out = call_opts.status_out orelse return;
    out.* = .{ .code = st.code, .message = try arena.dupe(u8, st.message) };
}

/// Same, for paths already returning an error. Propagating an allocator failure
/// here would replace the real reason the call failed, but `status_out` may be
/// uninitialized (callers pass `var st: Status = undefined`), so the code must
/// still be written — only the message degrades to empty.
fn writeStatusBestEffort(call_opts: call_mod.CallOptions, arena: std.mem.Allocator, st: call_mod.Status) void {
    const out = call_opts.status_out orelse return;
    out.* = .{ .code = st.code, .message = arena.dupe(u8, st.message) catch "" };
}

/// Records a mid-call transport failure. The RawCall's own mapping wins when
/// it has one (it saw the RST code / HTTP status / trailers); the raw error is
/// only a fallback for failures that never reached a status.
fn recordFailure(raw: *const call_mod.RawCall, call_opts: call_mod.CallOptions, arena: std.mem.Allocator, err: anyerror) void {
    writeStatusBestEffort(call_opts, arena, raw.stat orelse .{
        .code = status_mod.codeFromTransportError(err),
        .message = @errorName(err),
    });
}

/// Records a local codec failure (request encode or response decode). Unlike
/// transport errors, codec errors are never retryable on a fresh connection, so
/// they map to `.internal` (or `.resource_exhausted` on `OutOfMemory`).
fn recordCodecFailure(call_opts: call_mod.CallOptions, arena: std.mem.Allocator, err: anyerror) void {
    writeStatusBestEffort(call_opts, arena, .{
        .code = if (err == error.OutOfMemory) .resource_exhausted else .internal,
        .message = @errorName(err),
    });
}

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
    // Wait for the handler to finish recording the captured request fields.
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

// ---- connectTcp tests ----

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

fn canBindTcp4() bool {
    const fd = std.c.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    if (fd < 0) return false;
    defer _ = std.c.close(fd);
    var sin: std.posix.sockaddr.in = .{
        .family = std.posix.AF.INET,
        .port = 0,
        .addr = @bitCast([4]u8{ 127, 0, 0, 1 }),
    };
    return std.c.bind(fd, @ptrCast(&sin), @sizeOf(@TypeOf(sin))) == 0;
}

fn canBindTcp6() bool {
    const fd = std.c.socket(std.posix.AF.INET6, std.posix.SOCK.STREAM, 0);
    if (fd < 0) return false;
    defer _ = std.c.close(fd);
    var sin6: std.posix.sockaddr.in6 = .{
        .family = std.posix.AF.INET6,
        .port = 0,
        .flowinfo = 0,
        .addr = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
        .scope_id = 0,
    };
    return std.c.bind(fd, @ptrCast(&sin6), @sizeOf(@TypeOf(sin6))) == 0;
}

test "connectTcp dials h2c and completes a call" {
    if (!canBindTcp4()) return error.SkipZigTest;
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

test "connectTcp brackets IPv6 literals in :authority and accepts bracketed hosts (#7)" {
    const a_v6 = try formatDefaultAuthority(testing.allocator, "::1", 50051);
    defer testing.allocator.free(a_v6);
    try testing.expectEqualStrings("[::1]:50051", a_v6);

    const a_v4 = try formatDefaultAuthority(testing.allocator, "127.0.0.1", 50051);
    defer testing.allocator.free(a_v4);
    try testing.expectEqualStrings("127.0.0.1:50051", a_v4);

    if (!canBindTcp6()) return error.SkipZigTest;
    const io = testing.io;
    const addr6 = try std.Io.net.IpAddress.parse("::1", 0);
    for ([_][]const u8{ "::1", "[::1]" }) |dial_host| {
        var listener = try addr6.listen(io, .{ .mode = .stream, .reuse_address = true });
        defer listener.deinit(io);
        const port = listener.socket.address.getPort();

        var srv: h2.Server = .{ .io = io, .gpa = testing.allocator, .handler = tcpOkHandler };
        var tsrv: TcpSrv = .{ .io = io, .listener = &listener, .srv = &srv };
        const th = try std.Thread.spawn(.{}, TcpSrv.run, .{&tsrv});

        var chan: Channel = undefined;
        try chan.connectTcp(io, testing.allocator, dial_host, port, .{});

        var expected_buf: [32]u8 = undefined;
        const expected_auth = try std.fmt.bufPrint(&expected_buf, "[::1]:{d}", .{port});
        try testing.expectEqualStrings(expected_auth, chan.opts.authority);

        var call = try chan.startRaw("/test.Svc/Ok", .{});
        try call.sendMessage("x");
        try call.closeSend();
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        try testing.expect((try call.recvMessage(arena_state.allocator())) == null);
        try testing.expect((try call.finish()).isOk());
        call.deinit();
        chan.deinit();
        th.join();
    }
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

const codec_mod = @import("codec.zig");
const UnaryMsg = struct {
    text: []const u8 = "",

    pub fn encode(self: UnaryMsg, gpa: std.mem.Allocator) ![]u8 {
        return gpa.dupe(u8, self.text);
    }

    pub fn decode(arena: std.mem.Allocator, bytes: []const u8) !UnaryMsg {
        return .{ .text = try arena.dupe(u8, bytes) };
    }
};
const UnaryM = codec_mod.Method(UnaryMsg, UnaryMsg){ .path = "/test.Svc/Unary" };

test "unary reports a setup failure as a status, not just a Zig error" {
    // A peer GOAWAY makes the next openStream fail with error.GoingAway. The
    // caller needs to know that is retryable on a fresh connection, and an
    // error name does not say so — status_out must carry `unavailable`.
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var goaway: [8]u8 = @splat(0); // last_stream_id = 0, NO_ERROR
    try rp.writeFrame(.goaway, 0, 0, &goaway);

    // Wait for the client's reader to record it.
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 20) {
        if (rp.chan.h2c.goAway() != null) break;
        std.Io.sleep(testing.io, .{ .nanoseconds = 20 * std.time.ns_per_ms }, .awake) catch {};
    }

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var st: status_mod.Status = undefined;
    try testing.expectError(error.GoingAway, rp.chan.unary(
        UnaryM,
        arena_state.allocator(),
        .{ .text = "x" },
        .{ .status_out = &st },
    ));
    try testing.expectEqual(status_mod.Code.unavailable, st.code);
    try testing.expectEqualStrings("GoingAway", st.message);
}

test "unary reports the OK status too, not only failures" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, unaryOkHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var st: status_mod.Status = .{ .code = .unknown, .message = "untouched" };
    const reply = try lb.chan.unary(UnaryM, arena_state.allocator(), .{ .text = "ping" }, .{ .status_out = &st });
    try testing.expectEqualStrings("ping", reply.text);
    try testing.expectEqual(status_mod.Code.ok, st.code);
}

fn unaryOkHandler(ctx: *h2.Context) anyerror!void {
    var buf: [64]u8 = undefined;
    var n: usize = 0;
    if (ctx.body_reader) |br| {
        while (true) {
            const r = try br.read(buf[n..]);
            if (r == 0) break;
            n += r;
        }
    }
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    try ctx.res.write(buf[0..n]);
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
}

test "init refuses an empty authority instead of sending a request without one" {
    // hpack.Encoder.encodeRequest omits :authority when the value is empty, so
    // the old default produced requests a real gRPC server rejects, with
    // nothing here to explain why.
    //
    // Driven over a fixed reader/writer rather than a socket: the check happens
    // before the transport is touched, and this way an unfixed build (where
    // init succeeds) can be cleaned up and reported instead of leaving a reader
    // thread running past the end of this stack frame.
    var rbuf: [1]u8 = undefined;
    var r = std.Io.Reader.fixed(&rbuf);
    var wbuf: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&wbuf);

    var chan: Channel = undefined;
    if (chan.init(testing.io, testing.allocator, &r, &w, .{})) |_| {
        chan.deinit(); // the fixed reader is already at EOF, so the join returns
        return error.TestUnexpectedResult;
    } else |e| {
        try testing.expectEqual(error.AuthorityRequired, e);
    }
}

fn twoMessageHandler(ctx: *h2.Context) anyerror!void {
    var buf: [64]u8 = undefined;
    var n: usize = 0;
    if (ctx.body_reader) |br| {
        while (true) {
            const r = try br.read(buf[n..]);
            if (r == 0) break;
            n += r;
        }
    }
    ctx.res.status(200);
    try ctx.res.header("content-type", "application/grpc");
    // Two framed messages on a unary method — a server-side contract violation.
    var prefix: [5]u8 = .{ 0, 0, 0, 0, 2 };
    try ctx.res.write(&prefix);
    try ctx.res.write("aa");
    try ctx.res.write(&prefix);
    try ctx.res.write("bb");
    try ctx.res.trailer("grpc-status", "0");
    try ctx.res.finish();
}

test "unary rejects a response carrying more than one message" {
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, twoMessageHandler, null, .{});
    defer lb.stop();

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var st: status_mod.Status = undefined;
    try testing.expectError(error.TooManyResponses, lb.chan.unary(
        UnaryM,
        arena_state.allocator(),
        .{ .text = "x" },
        .{ .status_out = &st },
    ));
    // The protocol violation wins over the (OK) trailers the server sent.
    try testing.expectEqual(status_mod.Code.internal, st.code);
}

const UnaryRunner = struct {
    chan: *Channel,
    err: ?anyerror = null,
    code: status_mod.Code = .ok,
    msg_buf: [64]u8 = undefined,
    msg_len: usize = 0,
    done: std.atomic.Value(bool) = .init(false),

    fn run(self: *UnaryRunner) void {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var st: status_mod.Status = .{ .code = .ok };
        if (self.chan.unary(UnaryM, arena_state.allocator(), .{ .text = "payload" }, .{ .status_out = &st })) |_| {} else |e| {
            self.err = e;
        }
        self.code = st.code;
        const n = @min(st.message.len, self.msg_buf.len);
        @memcpy(self.msg_buf[0..n], st.message[0..n]);
        self.msg_len = n;
        self.done.store(true, .release);
    }
};

fn zeroInitialWindow(rp: *testutil.RawPeer) !void {
    var iw0: [6]u8 = undefined;
    h2.proto.putSetting(&iw0, h2.proto.set_initial_window_size, 0);
    try rp.writeFrame(.settings, 0, 0, &iw0);
    while (true) {
        const f = try rp.readFrame();
        const is_ack = f.hdr.ftype == .settings and (f.hdr.flags & h2.proto.flag_ack != 0);
        testing.allocator.free(f.payload);
        if (is_ack) break;
    }
}

test "unary surfaces unavailable (not unknown) when send hits GOAWAY refusal (#6)" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    // Force send() to block on flow control until GOAWAY(last_stream_id=0) refuses sid 1.
    try zeroInitialWindow(&rp);

    var runner: UnaryRunner = .{ .chan = &rp.chan };
    const th = try std.Thread.spawn(.{}, UnaryRunner.run, .{&runner});
    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);

    var ga: [8]u8 = @splat(0); // last_stream_id = 0, NO_ERROR
    try rp.writeFrame(.goaway, 0, 0, &ga);
    th.join();

    try testing.expectEqual(@as(?anyerror, error.RpcFailed), runner.err);
    try testing.expectEqual(status_mod.Code.unavailable, runner.code);
}

test "unary surfaces server Trailers-Only status when early rejection resets send (#6)" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    // Force send() to block while the server immediately answers Trailers-Only
    // (UNAUTHENTICATED) + RST_STREAM(NO_ERROR) per RFC 7540 §8.1.
    try zeroInitialWindow(&rp);

    var runner: UnaryRunner = .{ .chan = &rp.chan };
    const th = try std.Thread.spawn(.{}, UnaryRunner.run, .{&runner});
    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try blk.literal(testing.allocator, "grpc-status", "16");
    try blk.literal(testing.allocator, "grpc-message", "bad%20token");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, blk.buf.items);
    try rp.writeRst(sid, .no_error);
    th.join();

    try testing.expectEqual(@as(?anyerror, error.RpcFailed), runner.err);
    try testing.expectEqual(status_mod.Code.unauthenticated, runner.code);
    try testing.expectEqualStrings("bad token", runner.msg_buf[0..runner.msg_len]);
}

test "unary returns TooManyResponses immediately without waiting for trailers" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    var runner: UnaryRunner = .{ .chan = &rp.chan };
    const th = try std.Thread.spawn(.{}, UnaryRunner.run, .{&runner});
    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    // Send initial HEADERS + two messages and NEVER send trailers.
    // unary() must fail fast with TooManyResponses and emit RST_STREAM(CANCEL)
    // instead of hanging in finish() waiting for the stream to end.
    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);
    const two_msgs = [_]u8{ 0, 0, 0, 0, 2 } ++ "aa".* ++ [_]u8{ 0, 0, 0, 0, 2 } ++ "bb".*;
    try rp.writeFrame(.data, 0, sid, &two_msgs);

    // Wait for the RST_STREAM emitted when unary() bails out and deinit()s the call.
    const rst = try rp.readUntil(.rst_stream);
    testing.allocator.free(rst.payload);
    th.join();

    try testing.expectEqual(@as(?anyerror, error.TooManyResponses), runner.err);
    try testing.expectEqual(status_mod.Code.internal, runner.code);
}

test "unary preserves status_out.code and RpcFailed when arena OOMs on non-OK status" {
    var rp: testutil.RawPeer = undefined;
    try rp.start(testing.io, testing.allocator);
    defer rp.stop();

    const OomRunner = struct {
        chan: *Channel,
        err: ?anyerror = null,
        st: status_mod.Status = .{ .code = .ok, .message = "untouched" },

        fn run(self: *@This()) void {
            var fail = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
            if (self.chan.unary(
                UnaryM,
                fail.allocator(),
                .{ .text = "x" },
                .{ .status_out = &self.st },
            )) |_| {} else |e| {
                self.err = e;
            }
        }
    };

    var runner: OomRunner = .{ .chan = &rp.chan };
    const th = try std.Thread.spawn(.{}, OomRunner.run, .{&runner});
    const hf = try rp.readUntil(.headers);
    testing.allocator.free(hf.payload);
    const sid = hf.hdr.sid;

    var blk: testutil.RawPeer.HpackBlock = .{};
    defer blk.deinit(testing.allocator);
    try blk.status200(testing.allocator);
    try blk.literal(testing.allocator, "content-type", "application/grpc");
    try blk.literal(testing.allocator, "grpc-status", "5");
    try blk.literal(testing.allocator, "grpc-message", "missing");
    try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, blk.buf.items);
    th.join();

    try testing.expectEqual(@as(?anyerror, error.RpcFailed), runner.err);
    try testing.expectEqual(status_mod.Code.not_found, runner.st.code);
    try testing.expectEqualStrings("", runner.st.message);
}

test "startRaw and unary reject HTTP/2 connection-specific metadata (#15)" {
    var cap: Captured = .{};
    var lb: testutil.Loopback = undefined;
    try lb.start(testing.io, testing.allocator, captureHandler, &cap, .{});
    defer lb.stop();

    const banned = [_][]const u8{
        "connection",
        "Connection",
        "keep-alive",
        "proxy-connection",
        "transfer-encoding",
        "Transfer-Encoding",
        "upgrade",
        "UPGRADE",
    };
    for (banned) |name| {
        try testing.expectError(error.ReservedMetadataName, lb.chan.startRaw("/test.Svc/Meta", .{
            .metadata = &.{.{ .name = name, .value = "v" }},
        }));
    }

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var st: status_mod.Status = undefined;
    try testing.expectError(error.ReservedMetadataName, lb.chan.unary(
        UnaryM,
        arena_state.allocator(),
        .{ .text = "x" },
        .{
            .metadata = &.{.{ .name = "connection", .value = "close" }},
            .status_out = &st,
        },
    ));
    try testing.expectEqual(status_mod.Code.invalid_argument, st.code);
}

const ToggleFailWriter = struct {
    inner: *Io.Writer,
    fail: std.atomic.Value(bool) = .init(false),
    interface: Io.Writer = .{
        .vtable = &.{ .drain = drain, .flush = flush },
        .buffer = &.{},
    },

    fn drain(w: *Io.Writer, data: []const []const u8, splat: usize) Io.Writer.Error!usize {
        const self: *ToggleFailWriter = @fieldParentPtr("interface", w);
        if (self.fail.load(.acquire)) return error.WriteFailed;
        var total: usize = 0;
        for (data[0 .. data.len - 1]) |chunk| {
            try self.inner.writeAll(chunk);
            total += chunk.len;
        }
        const last = data[data.len - 1];
        var i: usize = 0;
        while (i < splat) : (i += 1) {
            try self.inner.writeAll(last);
            total += last.len;
        }
        return total;
    }

    fn flush(w: *Io.Writer) Io.Writer.Error!void {
        const self: *ToggleFailWriter = @fieldParentPtr("interface", w);
        if (self.fail.load(.acquire)) return error.WriteFailed;
        try self.inner.flush();
    }
};

fn readRawFrame(r: *Io.Reader, gpa: std.mem.Allocator) !testutil.RawPeer.Frame {
    var hb: [9]u8 = undefined;
    try r.readSliceAll(&hb);
    const hdr = h2.proto.parseHeader(&hb);
    const payload = try gpa.alloc(u8, hdr.length);
    errdefer gpa.free(payload);
    if (payload.len > 0) try r.readSliceAll(payload);
    return .{ .hdr = hdr, .payload = payload };
}

fn writeRawFrame(w: *Io.Writer, ftype: h2.proto.FrameType, flags: u8, sid: u31, payload: []const u8) !void {
    var hb: [9]u8 = undefined;
    h2.proto.putHeader(&hb, payload.len, ftype, flags, sid);
    try w.writeAll(&hb);
    if (payload.len > 0) try w.writeAll(payload);
    try w.flush();
}

test "unary maps send-path WriteFailed to unavailable (#14)" {
    const pair = try testutil.makeSocketPairStreams();
    var cstream = pair.client;
    var accepted = pair.server;
    defer {
        cstream.close(testing.io);
        accepted.close(testing.io);
    }

    var crbuf: [8192]u8 = undefined;
    var cwbuf: [8192]u8 = undefined;
    var arbuf: [8192]u8 = undefined;
    var awbuf: [8192]u8 = undefined;
    var csr = cstream.reader(testing.io, &crbuf);
    var csw = cstream.writer(testing.io, &cwbuf);
    var asr = accepted.reader(testing.io, &arbuf);
    var asw = accepted.writer(testing.io, &awbuf);

    var tw: ToggleFailWriter = .{ .inner = &csw.interface };
    var chan: Channel = undefined;
    try chan.init(testing.io, testing.allocator, &csr.interface, &tw.interface, .{ .authority = "raw" });
    defer {
        cstream.shutdown(testing.io, .both) catch {};
        accepted.shutdown(testing.io, .both) catch {};
        chan.deinit();
    }

    // Complete h2 handshake and force the initial stream send window to 0 so
    // openStream succeeds and Stream.send blocks until we release it.
    var pf: [h2.proto.preface.len]u8 = undefined;
    try asr.interface.readSliceAll(&pf);
    const cset = try readRawFrame(&asr.interface, testing.allocator);
    testing.allocator.free(cset.payload);

    var iw0: [6]u8 = undefined;
    h2.proto.putSetting(&iw0, h2.proto.set_initial_window_size, 0);
    try writeRawFrame(&asw.interface, .settings, 0, 0, &iw0);
    try writeRawFrame(&asw.interface, .settings, h2.proto.flag_ack, 0, "");
    while (true) {
        const f = try readRawFrame(&asr.interface, testing.allocator);
        const is_ack = f.hdr.ftype == .settings and (f.hdr.flags & h2.proto.flag_ack != 0);
        testing.allocator.free(f.payload);
        if (is_ack) break;
    }

    var runner: UnaryRunner = .{ .chan = &chan };
    const th = try std.Thread.spawn(.{}, UnaryRunner.run, .{&runner});

    // Wait until openStream has emitted HEADERS; sendMessage is now blocked on
    // flow control. Arm write failure and grant stream window so writeDataForStream
    // fails deterministically with error.WriteFailed.
    const sid = while (true) {
        const f = try readRawFrame(&asr.interface, testing.allocator);
        testing.allocator.free(f.payload);
        if (f.hdr.ftype == .headers) break f.hdr.sid;
    };
    tw.fail.store(true, .release);
    var wu: [4]u8 = undefined;
    std.mem.writeInt(u32, &wu, 1024, .big);
    try writeRawFrame(&asw.interface, .window_update, 0, sid, &wu);
    th.join();

    try testing.expectEqual(@as(?anyerror, error.WriteFailed), runner.err);
    try testing.expectEqual(status_mod.Code.unavailable, runner.code);
    try testing.expectEqualStrings("WriteFailed", runner.msg_buf[0..runner.msg_len]);
}

const BadDecodeMsg = struct {
    text: []const u8 = "",

    pub fn encode(self: BadDecodeMsg, gpa: std.mem.Allocator) ![]u8 {
        return gpa.dupe(u8, self.text);
    }

    pub fn decode(_: std.mem.Allocator, _: []const u8) !BadDecodeMsg {
        return error.BadMessage;
    }
};

const OomDecodeMsg = struct {
    pub fn encode(_: OomDecodeMsg, gpa: std.mem.Allocator) ![]u8 {
        return gpa.dupe(u8, "x");
    }

    pub fn decode(_: std.mem.Allocator, _: []const u8) !OomDecodeMsg {
        return error.OutOfMemory;
    }
};

const BadEncodeMsg = struct {
    pub fn encode(_: BadEncodeMsg, _: std.mem.Allocator) ![]u8 {
        // Deliberately use WriteFailed (which Writer.Allocating can emit on OOM)
        // to verify encode errors map to internal rather than transport unavailable.
        return error.WriteFailed;
    }

    pub fn decode(_: std.mem.Allocator, _: []const u8) !BadEncodeMsg {
        return .{};
    }
};

test "unary maps decode/encode failures to internal and prefers non-OK server status (#14)" {
    const BadDecodeM = codec_mod.Method(BadDecodeMsg, BadDecodeMsg){ .path = "/test.Svc/BadDecode" };
    const OomDecodeM = codec_mod.Method(OomDecodeMsg, OomDecodeMsg){ .path = "/test.Svc/OomDecode" };
    const BadEncodeM = codec_mod.Method(BadEncodeMsg, BadEncodeMsg){ .path = "/test.Svc/BadEncode" };

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // 1) OK response + decode failure -> error.BadMessage with status_out.code == .internal.
    {
        var lb: testutil.Loopback = undefined;
        try lb.start(testing.io, testing.allocator, unaryOkHandler, null, .{});
        defer lb.stop();

        var st: status_mod.Status = undefined;
        try testing.expectError(error.BadMessage, lb.chan.unary(
            BadDecodeM,
            arena,
            .{ .text = "payload" },
            .{ .status_out = &st },
        ));
        try testing.expectEqual(status_mod.Code.internal, st.code);
        try testing.expectEqualStrings("BadMessage", st.message);

        // OOM during decode maps to resource_exhausted.
        var oom_st: status_mod.Status = undefined;
        try testing.expectError(error.OutOfMemory, lb.chan.unary(
            OomDecodeM,
            arena,
            .{},
            .{ .status_out = &oom_st },
        ));
        try testing.expectEqual(status_mod.Code.resource_exhausted, oom_st.code);

        // Request encode failure maps to internal (never unavailable, even for WriteFailed).
        var enc_st: status_mod.Status = undefined;
        try testing.expectError(error.WriteFailed, lb.chan.unary(
            BadEncodeM,
            arena,
            .{},
            .{ .status_out = &enc_st },
        ));
        try testing.expectEqual(status_mod.Code.internal, enc_st.code);
        try testing.expectEqualStrings("WriteFailed", enc_st.message);
    }

    // 2) Non-OK server status wins over an undecodable payload.
    {
        var rp: testutil.RawPeer = undefined;
        try rp.start(testing.io, testing.allocator);
        defer rp.stop();

        const BadDecodeRunner = struct {
            chan: *Channel,
            err: ?anyerror = null,
            code: status_mod.Code = .ok,
            msg_buf: [64]u8 = undefined,
            msg_len: usize = 0,

            fn run(self: *@This()) void {
                var a = std.heap.ArenaAllocator.init(testing.allocator);
                defer a.deinit();
                var st: status_mod.Status = .{ .code = .ok };
                if (self.chan.unary(BadDecodeM, a.allocator(), .{ .text = "x" }, .{ .status_out = &st })) |_| {} else |e| {
                    self.err = e;
                }
                self.code = st.code;
                const n = @min(st.message.len, self.msg_buf.len);
                @memcpy(self.msg_buf[0..n], st.message[0..n]);
                self.msg_len = n;
            }
        };

        var runner: BadDecodeRunner = .{ .chan = &rp.chan };
        const th = try std.Thread.spawn(.{}, BadDecodeRunner.run, .{&runner});
        const hf = try rp.readUntil(.headers);
        testing.allocator.free(hf.payload);
        const sid = hf.hdr.sid;

        var blk: testutil.RawPeer.HpackBlock = .{};
        defer blk.deinit(testing.allocator);
        try blk.status200(testing.allocator);
        try blk.literal(testing.allocator, "content-type", "application/grpc");
        try rp.writeFrame(.headers, h2.proto.flag_end_headers, sid, blk.buf.items);

        const body = [_]u8{ 0, 0, 0, 0, 3 } ++ "bad".*;
        try rp.writeFrame(.data, 0, sid, &body);

        var tblk: testutil.RawPeer.HpackBlock = .{};
        defer tblk.deinit(testing.allocator);
        try tblk.literal(testing.allocator, "grpc-status", "5");
        try tblk.literal(testing.allocator, "grpc-message", "server%20err");
        try rp.writeFrame(.headers, h2.proto.flag_end_headers | h2.proto.flag_end_stream, sid, tblk.buf.items);
        th.join();

        try testing.expectEqual(@as(?anyerror, error.RpcFailed), runner.err);
        try testing.expectEqual(status_mod.Code.not_found, runner.code);
        try testing.expectEqualStrings("server err", runner.msg_buf[0..runner.msg_len]);
    }
}

test "connectTcp enables TCP_NODELAY by default and respects tcp_nodelay=false (#17)" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .wasi) return error.SkipZigTest;
    if (!canBindTcp4()) return error.SkipZigTest;

    const io = testing.io;
    for ([_]bool{ true, false }) |want_nodelay| {
        const addr0 = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
        var listener = try addr0.listen(io, .{ .mode = .stream, .reuse_address = true });
        defer listener.deinit(io);
        const port = listener.socket.address.ip4.port;

        var srv: h2.Server = .{ .io = io, .gpa = testing.allocator, .handler = tcpOkHandler };
        var tsrv: TcpSrv = .{ .io = io, .listener = &listener, .srv = &srv };
        const th = try std.Thread.spawn(.{}, TcpSrv.run, .{&tsrv});

        var chan: Channel = undefined;
        try chan.connectTcp(io, testing.allocator, "127.0.0.1", port, .{ .tcp_nodelay = want_nodelay });

        var val: c_int = -1;
        var len: std.posix.socklen_t = @sizeOf(c_int);
        const rc = std.c.getsockopt(
            chan.owned.?.stream.socket.handle,
            std.posix.IPPROTO.TCP,
            std.posix.TCP.NODELAY,
            &val,
            &len,
        );
        chan.deinit();
        th.join();

        try testing.expectEqual(@as(c_int, 0), rc);
        if (want_nodelay) {
            try testing.expect(val != 0);
        } else {
            try testing.expectEqual(@as(c_int, 0), val);
        }
    }
}
