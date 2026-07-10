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

    pub const Options = struct {
        authority: []const u8 = "",
        scheme: []const u8 = "http",
        user_agent: []const u8 = "grpc-zig/0.1",
        max_recv_message_size: u32 = 4 << 20, // grpc-go default
        max_send_message_size: u32 = std.math.maxInt(u32),
    };

    /// `self` must stay at a stable address until `deinit` (the h2 client's
    /// reader thread holds pointers into it). Caller owns `r`/`w`.
    pub fn init(self: *Channel, io: Io, gpa: std.mem.Allocator, r: *Io.Reader, w: *Io.Writer, opts: Options) !void {
        self.* = .{ .io = io, .gpa = gpa, .h2c = undefined, .opts = opts };
        try self.h2c.init(io, gpa, r, w);
    }

    pub fn deinit(self: *Channel) void {
        self.h2c.deinit();
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
