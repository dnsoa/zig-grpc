//! Interop client: exercises unary / server-stream / client-stream / bidi /
//! trailers-only against the grpc-go echo server (testdata/go-server) on
//! 127.0.0.1:50099. Exits 0 only if every scenario passes.

const std = @import("std");
const grpc = @import("zig_grpc");

const port = 50099;

/// Hand-rolled protobuf codec for grpc.examples.echo.{EchoRequest,EchoResponse}
/// (one field: `string message = 1`) — enough wire format for interop without
/// a proto runtime.
const EchoMsg = struct {
    message: []const u8 = "",

    pub fn encode(self: EchoMsg, gpa: std.mem.Allocator) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(gpa);
        if (self.message.len > 0) {
            try out.append(gpa, 0x0a); // field 1, wire type 2 (LEN)
            try appendVarint(&out, gpa, self.message.len);
            try out.appendSlice(gpa, self.message);
        }
        return out.toOwnedSlice(gpa);
    }

    pub fn decode(arena: std.mem.Allocator, bytes: []const u8) !EchoMsg {
        var msg: EchoMsg = .{};
        var i: usize = 0;
        while (i < bytes.len) {
            const tag = try readVarint(bytes, &i);
            const field = tag >> 3;
            switch (@as(u3, @truncate(tag))) {
                2 => {
                    const len = try readVarint(bytes, &i);
                    // Subtract rather than add: a crafted varint length makes
                    // `i + len` wrap, and the check would pass on the way to a
                    // bogus slice. `readVarint` leaves i <= bytes.len.
                    if (len > bytes.len - i) return error.Malformed;
                    if (field == 1) msg.message = try arena.dupe(u8, bytes[i..][0..len]);
                    i += len;
                },
                0 => _ = try readVarint(bytes, &i),
                5 => {
                    if (bytes.len - i < 4) return error.Malformed;
                    i += 4;
                },
                1 => {
                    if (bytes.len - i < 8) return error.Malformed;
                    i += 8;
                },
                else => return error.Malformed,
            }
        }
        return msg;
    }
};

fn appendVarint(out: *std.ArrayList(u8), gpa: std.mem.Allocator, v: usize) !void {
    var x = v;
    while (x >= 0x80) {
        try out.append(gpa, @as(u8, @truncate(x)) | 0x80);
        x >>= 7;
    }
    try out.append(gpa, @truncate(x));
}

fn readVarint(bytes: []const u8, i: *usize) !usize {
    var shift: u6 = 0;
    var v: usize = 0;
    while (i.* < bytes.len) {
        const b = bytes[i.*];
        i.* += 1;
        v |= @as(usize, b & 0x7f) << shift;
        if (b & 0x80 == 0) return v;
        shift += 7;
        if (shift > 56) return error.Malformed;
    }
    return error.Malformed;
}

const UnaryEcho = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/UnaryEcho" };
const ServerStream = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/ServerStreamingEcho" };
const ClientStream = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/ClientStreamingEcho" };
const BidiStream = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/BidirectionalStreamingEcho" };
const NoSuchMethod = grpc.Method(EchoMsg, EchoMsg){ .path = "/grpc.examples.echo.Echo/NoSuch" };

fn expect(cond: bool, comptime what: []const u8) !void {
    if (!cond) {
        std.debug.print("FAIL: {s}\n", .{what});
        return error.InteropFailed;
    }
    std.debug.print("PASS: {s}\n", .{what});
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    var chan: grpc.Channel = undefined;
    try chan.connectTcp(io, gpa, "127.0.0.1", port, .{});
    defer chan.deinit();

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // 1) unary
    {
        const reply = try chan.unary(UnaryEcho, arena, .{ .message = "hello-zig" }, .{});
        try expect(std.mem.eql(u8, reply.message, "hello-zig"), "unary echo");
    }

    // 2) server streaming: 1 req -> 3 replies "m-0","m-1","m-2"
    {
        var call = try chan.start(ServerStream, .{});
        defer call.deinit();
        try call.send(.{ .message = "m" });
        try call.closeSend();
        var n: usize = 0;
        while (try call.recv(arena)) |r| : (n += 1) {
            var buf: [8]u8 = undefined;
            const want = try std.fmt.bufPrint(&buf, "m-{d}", .{n});
            try expect(std.mem.eql(u8, r.message, want), "server-stream message");
        }
        try expect(n == 3, "server-stream count");
        try expect((try call.finish()).isOk(), "server-stream status");
    }

    // 3) client streaming: "a","b","c" -> "abc"
    {
        var call = try chan.start(ClientStream, .{});
        defer call.deinit();
        try call.send(.{ .message = "a" });
        try call.send(.{ .message = "b" });
        try call.send(.{ .message = "c" });
        try call.closeSend();
        const r = (try call.recv(arena)).?;
        try expect(std.mem.eql(u8, r.message, "abc"), "client-stream concat");
        try expect((try call.recv(arena)) == null, "client-stream single reply");
        try expect((try call.finish()).isOk(), "client-stream status");
    }

    // 4) bidi ping-pong
    {
        var call = try chan.start(BidiStream, .{});
        defer call.deinit();
        const msgs = [_][]const u8{ "p1", "p2", "p3" };
        for (msgs) |m| {
            try call.send(.{ .message = m });
            const r = (try call.recv(arena)).?;
            try expect(std.mem.eql(u8, r.message, m), "bidi echo");
        }
        try call.closeSend();
        try expect((try call.recv(arena)) == null, "bidi end");
        try expect((try call.finish()).isOk(), "bidi status");
    }

    // 5) unknown method -> Trailers-Only UNIMPLEMENTED (verified against real grpc-go)
    {
        var st: grpc.Status = undefined;
        const r = chan.unary(NoSuchMethod, arena, .{ .message = "x" }, .{ .status_out = &st });
        try expect(r == error.RpcFailed, "unknown method fails");
        try expect(st.code == .unimplemented, "unknown method is UNIMPLEMENTED");
    }

    std.debug.print("interop: all scenarios passed\n", .{});
}
